import asyncio
from ..env import LOG, DEFAULT_CORE_CONFIG
from ..infra.db import DB_CLIENT
from ..infra.async_mq import (
    register_consumer,
    publish_mq,
    Message,
    ConsumerConfigData,
    SpecialHandler,
)
from ..telemetry.log import get_wide_event, set_wide_event, clear_wide_event
from ..schema.mq.session import InsertNewMessage
from ..schema.utils import asUUID
from ..schema.result import Result
from .constants import EX, RK
from .data import message as MD
from .data import project as PD
from .controller import message as MC
from .utils import (
    check_redis_lock_or_set,
    release_redis_lock,
    check_buffer_timer_or_set,
)


async def waiting_for_message_notify(wait_for_seconds: int, body: InsertNewMessage):
    LOG.debug(
        "session.buffer_timer_waiting",
        session_id=str(body.session_id),
        wait_seconds=wait_for_seconds,
    )
    await asyncio.sleep(wait_for_seconds)
    timer_body = InsertNewMessage(
        project_id=body.project_id,
        session_id=body.session_id,
        message_id=body.message_id,
        skip_latest_check=True,
    )
    await publish_mq(
        exchange_name=EX.session_message,
        routing_key=RK.session_message_buffer_process,
        body=timer_body.model_dump_json(),
    )


@register_consumer(
    config=ConsumerConfigData(
        exchange_name=EX.session_message,
        routing_key=RK.session_message_insert,
        queue_name="session.message.insert.entry",
    )
)
async def insert_new_message(body: InsertNewMessage, message: Message):
    wide = get_wide_event()

    async with DB_CLIENT.get_session_context() as read_session:
        r = await MD.get_message_ids(read_session, body.session_id)
        message_ids, eil = r.unpack()
        if eil:
            return
        if not len(message_ids):
            if wide is not None:
                wide["action"] = "skip_no_pending"
                wide["_log_level"] = "debug"
            return
        latest_pending_message_id = message_ids[0]
        if not body.skip_latest_check and body.message_id != latest_pending_message_id:
            if wide is not None:
                wide["action"] = "skip_not_latest"
                wide["_log_level"] = "debug"
            return

        r = await PD.get_project_config(read_session, body.project_id)
        project_config, eil = r.unpack()
        if eil:
            return

        r = await MD.session_message_length(read_session, body.session_id)
        pending_message_length, eil = r.unpack()
        if eil:
            return

        if wide is not None:
            wide["pending_message_count"] = pending_message_length

        if (
            pending_message_length
            < project_config.project_session_message_buffer_max_turns
        ):
            should_create_timer = await check_buffer_timer_or_set(
                body.project_id,
                body.session_id,
                project_config.project_session_message_buffer_ttl_seconds,
            )
            if wide is not None:
                wide["timer_created"] = should_create_timer
                wide["action"] = "timer_wait"
                wide["_log_level"] = "debug"
            if should_create_timer:
                asyncio.create_task(
                    waiting_for_message_notify(
                        project_config.project_session_message_buffer_ttl_seconds,
                        body,
                    )
                )
            return

    body.skip_latest_check = False
    _l = await check_redis_lock_or_set(
        body.project_id, f"session.message.insert.{body.session_id}"
    )
    if not _l:
        if wide is not None:
            wide["lock_acquired"] = False
            wide["action"] = "retry_locked"
            wide["_log_level"] = "debug"
        body.lock_retry_count += 1
        await publish_mq(
            exchange_name=EX.session_message,
            routing_key=RK.session_message_insert_retry,
            body=body.model_dump_json(),
        )
        return

    if wide is not None:
        wide["lock_acquired"] = True
        wide["lock_retries"] = body.lock_retry_count
        wide["buffer_full"] = True

    try:
        if pending_message_length > (
            project_config.project_session_message_buffer_max_overflow
            + project_config.project_session_message_buffer_max_turns
        ):
            if wide is not None:
                wide["buffer_overflow"] = True
                wide["action"] = "overflow_truncate"
            await publish_mq(
                exchange_name=EX.session_message,
                routing_key=RK.session_message_insert_retry,
                body=body.model_dump_json(),
            )
        else:
            if wide is not None:
                wide["action"] = "process"
        await MC.process_session_pending_message(
            project_config, body.project_id, body.session_id
        )
    finally:
        await release_redis_lock(
            body.project_id, f"session.message.insert.{body.session_id}"
        )


register_consumer(
    config=ConsumerConfigData(
        exchange_name=EX.session_message,
        routing_key=RK.session_message_insert_retry,
        queue_name="session.message.insert.retry",
        message_ttl_seconds=DEFAULT_CORE_CONFIG.session_message_session_lock_wait_seconds,
        need_dlx_queue=True,
        use_dlx_ex_rk=(EX.session_message, RK.session_message_insert),
    )
)(SpecialHandler.NO_PROCESS)


@register_consumer(
    config=ConsumerConfigData(
        exchange_name=EX.session_message,
        routing_key=RK.session_message_buffer_process,
        queue_name="session.message.buffer.process",
    )
)
async def buffer_new_message(body: InsertNewMessage, message: Message):
    wide = get_wide_event()

    async with DB_CLIENT.get_session_context() as session:
        r = await MD.get_message_ids(session, body.session_id)
        message_ids, eil = r.unpack()
        if eil:
            return
        if not len(message_ids):
            if wide is not None:
                wide["action"] = "skip_no_pending"
                wide["_log_level"] = "debug"
            return
        latest_pending_message_id = message_ids[0]
        if not body.skip_latest_check and body.message_id != latest_pending_message_id:
            if wide is not None:
                wide["action"] = "skip_not_latest"
                wide["_log_level"] = "debug"
            return
        r = await PD.get_project_config(session, body.project_id)
        project_config, eil = r.unpack()
        if eil:
            return

    if wide is not None:
        wide["action"] = "process"

    body.skip_latest_check = False
    _l = await check_redis_lock_or_set(
        body.project_id, f"session.message.insert.{body.session_id}"
    )
    if not _l:
        if wide is not None:
            wide["lock_acquired"] = False
            wide["action"] = "retry_locked"
            wide["_log_level"] = "debug"
        body.lock_retry_count += 1
        await publish_mq(
            exchange_name=EX.session_message,
            routing_key=RK.session_message_insert_retry,
            body=body.model_dump_json(),
        )
        return

    if wide is not None:
        wide["lock_acquired"] = True
        wide["lock_retries"] = body.lock_retry_count

    try:
        await MC.process_session_pending_message(
            project_config, body.project_id, body.session_id
        )
    finally:
        await release_redis_lock(
            body.project_id, f"session.message.insert.{body.session_id}"
        )


async def flush_session_message_blocking(
    project_id: asUUID, session_id: asUUID
) -> Result[None]:
    from time import perf_counter

    wide_event: dict = {
        "handler": "flush_session_message_blocking",
        "session_id": str(session_id),
        "project_id": str(project_id),
    }
    set_wide_event(wide_event)
    _start = perf_counter()

    max_retries = DEFAULT_CORE_CONFIG.session_message_flush_max_retries
    try:
        for _attempt in range(max_retries):
            _l = await check_redis_lock_or_set(
                project_id, f"session.message.insert.{session_id}"
            )
            if _l:
                break
            await asyncio.sleep(
                DEFAULT_CORE_CONFIG.session_message_session_lock_wait_seconds
            )
        else:
            wide_event["outcome"] = "retries_exhausted"
            wide_event["lock_retries"] = max_retries
            return Result.reject(
                f"Failed to acquire session lock after {max_retries} retries"
            )

        wide_event["lock_retries"] = _attempt

        try:
            async with DB_CLIENT.get_session_context() as read_session:
                r = await PD.get_project_config(read_session, project_id)
                project_config, eil = r.unpack()
                if eil:
                    wide_event["outcome"] = "error"
                    wide_event["error"] = str(eil)
                    return r
            r = await MC.process_session_pending_message(
                project_config, project_id, session_id
            )
            wide_event["outcome"] = "success" if r.ok() else "failed"
            return r
        finally:
            await release_redis_lock(
                project_id, f"session.message.insert.{session_id}"
            )
    finally:
        wide_event["duration_ms"] = round(
            (perf_counter() - _start) * 1000, 2
        )
        LOG.info("flush.message.processed", **wide_event)
        clear_wide_event()
