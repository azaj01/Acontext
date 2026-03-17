# NOTE: MQ connection may be closed after long idle time or during startup instability.
# The publish() method includes retry logic to handle reconnection automatically.
import asyncio
import json
import traceback
from enum import StrEnum
from functools import partial
from pydantic import ValidationError, BaseModel
from dataclasses import dataclass, field
from typing import Callable, Awaitable, Any, Dict, Optional, List, Set
from time import perf_counter

from aio_pika import connect_robust, ExchangeType, Message
from aio_pika.exceptions import ChannelInvalidStateError
from aio_pika.abc import AbstractConnection, AbstractChannel, AbstractQueue

from ..env import LOG, DEFAULT_CORE_CONFIG
from ..telemetry.log import (
    bound_logging_vars,
    set_wide_event,
    clear_wide_event,
)
from ..util.handler_spec import check_handler_function_sanity, get_handler_body_type

# OpenTelemetry imports for manual context propagation
try:
    from opentelemetry import trace, propagate, context as otel_context

    OTEL_AVAILABLE = True
except ImportError:
    OTEL_AVAILABLE = False


def _extract_trace_context_from_headers(message: Message) -> Optional[Any]:
    """Extract trace context from message headers for distributed tracing."""
    if not OTEL_AVAILABLE or not message.headers:
        return None

    try:
        from ..telemetry.config import TelemetryConfig

        config = TelemetryConfig.from_env()
        if not config.enabled:
            return None

        headers = {}
        for k, v in message.headers.items():
            if isinstance(v, (str, bytes)):
                headers[k] = (
                    v if isinstance(v, str) else v.decode("utf-8", errors="ignore")
                )
            else:
                headers[k] = str(v)

        if headers:
            return propagate.extract(headers)
    except Exception:
        pass

    return None


class SpecialHandler(StrEnum):
    NO_PROCESS = "no_process"


LOGGING_FIELDS = {"project_id", "session_id", "task_id", "learning_space_id"}

_FRAMEWORK_WIDE_EVENT_KEYS = frozenset(
    {
        "queue_name",
        "handler",
        "retry_attempt",
        "outcome",
        "error",
        "validation_error",
        "timeout_seconds",
        "duration_ms",
        "_log_level",
    }
)


@dataclass
class ConsumerConfigData:
    """Configuration for a single consumer"""

    exchange_name: str
    routing_key: str
    queue_name: str
    exchange_type: ExchangeType = ExchangeType.DIRECT
    durable: bool = True
    auto_delete: bool = False
    # Configuration
    prefetch_count: int = DEFAULT_CORE_CONFIG.mq_global_qos
    message_ttl_seconds: int = DEFAULT_CORE_CONFIG.mq_default_message_ttl_seconds
    timeout: float = DEFAULT_CORE_CONFIG.mq_consumer_handler_timeout
    max_retries: int = DEFAULT_CORE_CONFIG.mq_default_max_retries
    retry_delay: float = DEFAULT_CORE_CONFIG.mq_default_retry_delay_unit_sec
    # DLX setup
    need_dlx_queue: bool = False
    dlx_ttl_days: int = DEFAULT_CORE_CONFIG.mq_default_dlx_ttl_days
    use_dlx_ex_rk: Optional[tuple[str, str]] = None
    dlx_suffix: str = "dead"


@dataclass
class ConsumerConfig(ConsumerConfigData):
    """Configuration for a single consumer"""

    handler: Optional[
        Callable[[BaseModel, Message], Awaitable[Any]] | SpecialHandler
    ] = field(default=None)
    body_pydantic_type: Optional[BaseModel] = field(default=None)

    def __post_init__(self):
        assert self.handler is not None, "Consumer Handler can not be None"
        if isinstance(self.handler, SpecialHandler):
            return
        _, eil = check_handler_function_sanity(self.handler).unpack()
        if eil:
            raise ValueError(
                f"Handler function {self.handler} does not meet the sanity requirements:\n{eil}"
            )

        self.body_pydantic_type = get_handler_body_type(self.handler)
        assert self.body_pydantic_type is not None, "Handler body type can not be None"


@dataclass
class ConnectionConfig:
    """MQ connection configuration"""

    url: str
    connection_name: str = DEFAULT_CORE_CONFIG.mq_connection_name
    heartbeat: int = DEFAULT_CORE_CONFIG.mq_heartbeat
    blocked_connection_timeout: int = DEFAULT_CORE_CONFIG.mq_blocked_connection_timeout


class AsyncSingleThreadMQConsumer:
    """
    High-performance async MQ consumer with runtime registration

    Features:
    - Runtime consumer registration
    - Efficient connection pooling
    - Automatic reconnection
    - Error handling and retry logic
    - Dead letter queue support
    - Graceful shutdown
    - Concurrent message processing
    """

    def __init__(self, connection_config: ConnectionConfig):
        self.connection_config = connection_config
        self.connection: Optional[AbstractConnection] = None
        self.consumers: Dict[str, ConsumerConfig] = {}
        self._publish_channle: Optional[AbstractChannel] = None
        self._consumer_loop_tasks: List[asyncio.Task] = []
        self._shutdown_event = asyncio.Event()
        self._processing_tasks: Set[asyncio.Task] = set()
        self.__running = False
        self._connection_lock = asyncio.Lock()  # Lock for connection operations
        self._stop_lock = asyncio.Lock()

    @property
    def running(self) -> bool:
        return self.__running

    async def connect(self) -> None:
        """Establish connection to MQ"""
        if self.connection and not self.connection.is_closed:
            return

        async with self._connection_lock:
            if self.connection and not self.connection.is_closed:
                return

            try:
                self.connection = await connect_robust(
                    self.connection_config.url,
                    client_properties={
                        "connection_name": self.connection_config.connection_name
                    },
                    heartbeat=self.connection_config.heartbeat,
                    blocked_connection_timeout=self.connection_config.blocked_connection_timeout,
                )
                self._publish_channle = await self.connection.channel()
                LOG.info(
                    "mq.connected",
                    connection_name=self.connection_config.connection_name,
                )
            except Exception as e:
                LOG.error("mq.connect_failed", error=str(e))
                raise e

    async def disconnect(self) -> None:
        """Close connection to MQ"""
        if self._publish_channle and not self._publish_channle.is_closed:
            await self._publish_channle.close()
            self._publish_channle = None
        if self.connection and not self.connection.is_closed:
            await self.connection.close()
            self.connection = None
        LOG.info("mq.disconnected")

    def register_consumer(self, consumer_config: ConsumerConfig) -> None:
        """Register a consumer at runtime"""
        if self.running:
            raise RuntimeError(
                "Cannot register consumers while the consumer is running"
            )

        self.consumers[consumer_config.queue_name] = consumer_config

    async def _process_message(
        self,
        config: ConsumerConfig,
        message: Message,
    ) -> None:
        """Process a single message with retry logic and wide event emission."""
        extracted_context = _extract_trace_context_from_headers(message)

        handler_name = (
            config.handler.__name__ if callable(config.handler) else str(config.handler)
        )
        wide_event: dict = {
            "queue_name": config.queue_name,
            "handler": handler_name,
        }
        set_wide_event(wide_event)
        _start_total = perf_counter()

        async with message.process(requeue=False, ignore_processed=True):
            retry_count = 0
            max_retries = config.max_retries

            try:
                while retry_count <= max_retries:
                    handler_keys = set(wide_event.keys()) - _FRAMEWORK_WIDE_EVENT_KEYS
                    for k in handler_keys:
                        del wide_event[k]
                    wide_event["retry_attempt"] = retry_count

                    try:
                        try:
                            payload = json.loads(message.body.decode("utf-8"))
                            validated_body = config.body_pydantic_type.model_validate(
                                payload
                            )
                            _logging_vars = {
                                k: payload.get(k, None) for k in LOGGING_FIELDS
                            }
                            with bound_logging_vars(
                                queue_name=config.queue_name, **_logging_vars
                            ):
                                if extracted_context and OTEL_AVAILABLE:
                                    token = otel_context.attach(extracted_context)
                                    try:
                                        await asyncio.wait_for(
                                            config.handler(validated_body, message),
                                            timeout=config.timeout,
                                        )
                                    finally:
                                        otel_context.detach(token)
                                else:
                                    await asyncio.wait_for(
                                        config.handler(validated_body, message),
                                        timeout=config.timeout,
                                    )

                                wide_event["outcome"] = "success"
                        except ValidationError as e:
                            wide_event["outcome"] = "validation_error"
                            wide_event["validation_error"] = str(e)
                            await message.reject(requeue=False)
                            return
                        except asyncio.TimeoutError:
                            raise TimeoutError(
                                f"Handler timeout after {config.timeout}s"
                            )

                        return

                    except Exception as e:
                        retry_count += 1
                        _wait_for = config.retry_delay * (retry_count**2)

                        if retry_count <= max_retries:
                            wide_event["retry_attempt"] = retry_count
                            wide_event["retry_wait_seconds"] = _wait_for
                            await asyncio.sleep(_wait_for)
                        else:
                            if isinstance(e, TimeoutError):
                                wide_event["outcome"] = "timeout"
                                wide_event["timeout_seconds"] = config.timeout
                            else:
                                wide_event["outcome"] = "error"
                            wide_event["error"] = {
                                "type": type(e).__name__,
                                "message": str(e),
                            }
                            await message.reject(requeue=False)
                            return
            finally:
                wide_event["duration_ms"] = round(
                    (perf_counter() - _start_total) * 1000, 2
                )
                _emit_level = wide_event.pop("_log_level", "info")
                getattr(LOG, _emit_level, LOG.info)(
                    "mq.message.processed", **wide_event
                )
                clear_wide_event()

    def cleanup_message_task(self, consumer_name: str, task: asyncio.Task) -> None:
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except ChannelInvalidStateError as e:
            LOG.error(
                "mq.channel_invalid",
                consumer=consumer_name,
                error=str(e),
            )
        except Exception as e:
            LOG.error(
                "mq.task_error",
                consumer=consumer_name,
                error=str(e),
            )
        finally:
            self._processing_tasks.discard(task)

    async def _process_message_with_tracing(
        self, config: ConsumerConfig, message: Message
    ) -> None:
        """Process a message with tracing."""
        return await self._process_message(config, message)

    async def _special_queue(self, config: ConsumerConfig) -> str:
        if config.handler is SpecialHandler.NO_PROCESS:
            return f"Special consumer - queue: {config.queue_name} <- ({config.exchange_name}, {config.routing_key}), {config.handler}."
        raise RuntimeError(f"Special handler {config.handler} not implemented")

    async def _consume_queue(self, config: ConsumerConfig) -> str:
        """Consume messages from a specific queue with automatic channel reconnection"""

        max_reconnect_attempts = DEFAULT_CORE_CONFIG.mq_max_reconnect_attempts
        reconnect_delay = DEFAULT_CORE_CONFIG.mq_reconnect_delay
        attempt = 0

        while not self._shutdown_event.is_set():
            consumer_channel: AbstractChannel | None = None
            try:
                if not self.connection or self.connection.is_closed:
                    LOG.warning(
                        "mq.connection_lost",
                        queue_name=config.queue_name,
                    )
                    await self.connect()

                consumer_channel = await self.connection.channel()
                await consumer_channel.set_qos(prefetch_count=config.prefetch_count)
                queue = await self._setup_consumer_on_channel(config, consumer_channel)

                attempt = 0

                if isinstance(config.handler, SpecialHandler):
                    hint = await self._special_queue(config)
                    return hint

                async with queue.iterator() as queue_iter:
                    async for message in queue_iter:
                        if self._shutdown_event.is_set():
                            break

                        task = asyncio.create_task(
                            self._process_message_with_tracing(config, message)
                        )
                        self._processing_tasks.add(task)
                        task.add_done_callback(
                            partial(
                                self.cleanup_message_task,
                                config.queue_name,
                            )
                        )

                if self._shutdown_event.is_set():
                    break

            except asyncio.CancelledError:
                LOG.info("mq.consumer_cancelled", queue_name=config.queue_name)
                raise
            except Exception as e:
                attempt += 1
                if attempt > max_reconnect_attempts:
                    LOG.error(
                        "mq.consumer_failed",
                        queue_name=config.queue_name,
                        error=str(e),
                        attempts=max_reconnect_attempts,
                    )
                    raise e
                _delay_seconds = reconnect_delay * (attempt**2)
                LOG.info(
                    "mq.consumer_reconnecting",
                    queue_name=config.queue_name,
                    error=str(e),
                    attempt=attempt,
                    max_attempts=max_reconnect_attempts,
                    delay_seconds=_delay_seconds,
                )
                await asyncio.sleep(_delay_seconds)

            finally:
                if consumer_channel and not consumer_channel.is_closed:
                    try:
                        await consumer_channel.close()
                    except Exception:
                        pass

    async def _setup_consumer_on_channel(
        self,
        config: ConsumerConfig,
        channel: AbstractChannel,
    ) -> AbstractQueue:
        """Setup exchange, queue, and bindings for a consumer on a specific channel"""
        exchange = await channel.declare_exchange(
            config.exchange_name, config.exchange_type, durable=config.durable
        )
        queue_arguments: dict = {
            "x-message-ttl": config.message_ttl_seconds * 1000,
        }
        if config.need_dlx_queue and config.use_dlx_ex_rk is None:
            dlx_exchange_name = f"{config.exchange_name}.{config.dlx_suffix}"
            dlx_routing_key = f"{config.routing_key}.{config.dlx_suffix}"
            dlq_name = f"{config.queue_name}.{config.dlx_suffix}"

            dlx = await channel.declare_exchange(
                dlx_exchange_name, ExchangeType.DIRECT, durable=True
            )

            dlq = await channel.declare_queue(
                dlq_name,
                durable=True,
                arguments={"x-message-ttl": 24 * 60 * 60 * config.dlx_ttl_days * 1000},
            )
            await dlq.bind(dlx, dlx_routing_key)

            queue_arguments["x-dead-letter-exchange"] = dlx_exchange_name
            queue_arguments["x-dead-letter-routing-key"] = dlx_routing_key

        if config.need_dlx_queue and config.use_dlx_ex_rk is not None:
            queue_arguments["x-dead-letter-exchange"] = config.use_dlx_ex_rk[0]
            queue_arguments["x-dead-letter-routing-key"] = config.use_dlx_ex_rk[1]

        queue = await channel.declare_queue(
            config.queue_name,
            durable=config.durable,
            auto_delete=config.auto_delete,
            arguments=queue_arguments,
        )

        await queue.bind(exchange, config.routing_key)

        return queue

    async def _force_reconnect(self) -> None:
        """Force a full reconnection, safely closing old connection if possible"""
        async with self._connection_lock:
            LOG.info("mq.force_reconnect")

            old_connection = self.connection
            self._publish_channle = None
            self.connection = None

            if old_connection:
                try:
                    if not old_connection.is_closed:
                        await old_connection.close()
                except Exception:
                    pass

            try:
                self.connection = await connect_robust(
                    self.connection_config.url,
                    client_properties={
                        "connection_name": self.connection_config.connection_name
                    },
                    heartbeat=self.connection_config.heartbeat,
                    blocked_connection_timeout=self.connection_config.blocked_connection_timeout,
                )
                self._publish_channle = await self.connection.channel()
                LOG.info("mq.reconnect_success")
            except Exception as e:
                LOG.error("mq.reconnect_failed", error=str(e))
                raise

    async def _ensure_publish_channel(self) -> None:
        """Ensure we have a valid publish channel, reconnecting if necessary"""
        if self.connection is None or self.connection.is_closed:
            LOG.info("mq.publish_channel_reconnecting")
            self._publish_channle = None
            await self.connect()
            return

        if self._publish_channle is None or self._publish_channle.is_closed:
            try:
                self._publish_channle = await self.connection.channel()
            except RuntimeError as e:
                if "closed" in str(e).lower():
                    LOG.info(
                        "mq.connection_stale",
                        error=str(e),
                    )
                    await self._force_reconnect()
                else:
                    raise

    async def publish(self, exchange_name: str, routing_key: str, body: str) -> None:
        """Publish a message to an exchange with OpenTelemetry tracing."""
        assert len(exchange_name) and len(routing_key)

        tracer = span = None
        if OTEL_AVAILABLE:
            try:
                tracer = trace.get_tracer(__name__)
                span = tracer.start_span(
                    f"{exchange_name} publish",
                    kind=trace.SpanKind.PRODUCER,
                )
                span.set_attribute("messaging.system", "rabbitmq")
                span.set_attribute("messaging.destination.name", exchange_name)
                span.set_attribute(
                    "messaging.rabbitmq.destination.routing_key", routing_key
                )
                span.set_attribute("messaging.operation", "publish")
                span.set_attribute(
                    "messaging.message.body.size", len(body.encode("utf-8"))
                )
            except Exception:
                span = None

        try:
            headers = {}
            if OTEL_AVAILABLE:
                try:
                    from ..telemetry.config import TelemetryConfig

                    config = TelemetryConfig.from_env()
                    if config.enabled:
                        if span:
                            ctx = trace.set_span_in_context(span)
                            propagate.inject(headers, context=ctx)
                        else:
                            propagate.inject(headers)
                except Exception:
                    pass

            max_retries = 3
            retry_delay = 1.0
            last_exception = None

            for attempt in range(max_retries):
                try:
                    await self._ensure_publish_channel()

                    if self._publish_channle is None:
                        raise RuntimeError(
                            "No active MQ Publish Channel after reconnection"
                        )

                    message = Message(
                        body.encode("utf-8"),
                        content_type="application/json",
                        delivery_mode=2,
                        headers=headers if headers else None,
                    )

                    exchange = await self._publish_channle.get_exchange(exchange_name)
                    await exchange.publish(message, routing_key=routing_key)

                    if span:
                        span.set_status(trace.Status(trace.StatusCode.OK))
                    return

                except Exception as e:
                    last_exception = e
                    is_connection_error = "closed" in str(e).lower() or isinstance(
                        e, (ConnectionError, RuntimeError)
                    )

                    if is_connection_error and attempt < max_retries - 1:
                        wait_time = retry_delay * (attempt + 1)
                        LOG.info(
                            "mq.publish_retry",
                            attempt=attempt + 1,
                            max_retries=max_retries,
                            wait_seconds=wait_time,
                            error=str(e),
                        )
                        self._publish_channle = None
                        await asyncio.sleep(wait_time)
                    else:
                        if span:
                            span.record_exception(e)
                            span.set_status(
                                trace.Status(trace.StatusCode.ERROR, str(e))
                            )
                        raise

            if last_exception:
                if span:
                    span.record_exception(last_exception)
                    span.set_status(
                        trace.Status(trace.StatusCode.ERROR, str(last_exception))
                    )
                raise last_exception
        finally:
            if span:
                span.end()

    async def start(self) -> None:
        """Start all registered consumers"""
        if self.running:
            raise RuntimeError("Consumer is already running")

        if not self.consumers:
            raise RuntimeError("No consumers registered")

        if not self.connection or self.connection.is_closed:
            await self.connect()

        self.__running = True
        self._shutdown_event.clear()

        for config in self.consumers.values():
            task = asyncio.create_task(self._consume_queue(config))
            self._consumer_loop_tasks.append(task)

        LOG.info("mq.consumers_started", count=len(self.consumers))
        try:
            while not self._shutdown_event.is_set():
                done, pending = await asyncio.wait(
                    self._consumer_loop_tasks
                    + [asyncio.create_task(self._shutdown_event.wait())],
                    return_when=asyncio.FIRST_COMPLETED,
                )

                for task in done:
                    try:
                        r = task.result()
                        if task in self._consumer_loop_tasks:
                            self._consumer_loop_tasks.remove(task)
                    except Exception as e:
                        LOG.error(
                            "mq.consumer_task_failed",
                            error=str(e),
                            remaining=len(self._consumer_loop_tasks),
                        )
                        return
            LOG.info("mq.shutdown_received")
        finally:
            await self.stop()

    async def stop_current_tasks(self) -> None:
        self._shutdown_event.set()
        LOG.info("mq.stopping_consumers", count=len(self._consumer_loop_tasks))
        for task in self._consumer_loop_tasks:
            task.cancel()

        if self._consumer_loop_tasks:
            await asyncio.gather(*self._consumer_loop_tasks, return_exceptions=True)

        LOG.info("mq.stopping_tasks", count=len(self._processing_tasks))
        if self._processing_tasks:
            for task in list(self._processing_tasks):
                task.cancel()
            await asyncio.gather(*self._processing_tasks, return_exceptions=True)
            self._processing_tasks.clear()

        self._consumer_loop_tasks.clear()

    async def stop(self) -> None:
        """Stop all consumers gracefully"""
        async with self._stop_lock:
            if self.running:
                await self.stop_current_tasks()

            self.__running = False
            await self.disconnect()
            LOG.info("mq.all_stopped")

    async def health_check(self) -> bool:
        """Check if the consumer is healthy"""
        await self.connect()
        if not self.connection or self.connection.is_closed:
            return False
        return True


MQ_CLIENT = AsyncSingleThreadMQConsumer(
    ConnectionConfig(
        url=DEFAULT_CORE_CONFIG.mq_url,
        connection_name=DEFAULT_CORE_CONFIG.mq_connection_name,
    )
)


def register_consumer(config: ConsumerConfigData):
    """Decorator to register a function as a message handler"""

    def decorator(func: Callable[[dict, Message], Awaitable[Any]] | SpecialHandler):
        _consumer_config = ConsumerConfig(**config.__dict__, handler=func)
        MQ_CLIENT.register_consumer(_consumer_config)
        return func

    return decorator


async def publish_mq(exchange_name: str, routing_key: str, body: str) -> None:
    await MQ_CLIENT.publish(exchange_name, routing_key, body)


async def init_mq() -> None:
    """Initialize MQ connection (perform health check)."""
    if await MQ_CLIENT.health_check():
        LOG.info("mq.init_success")
    else:
        LOG.error("mq.init_failed")
        raise ConnectionError("Could not connect to MQ")
    await MQ_CLIENT.connect()


async def start_mq() -> None:
    await MQ_CLIENT.start()


async def close_mq() -> None:
    await MQ_CLIENT.stop()
