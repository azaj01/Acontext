from typing import final
import uuid
from functools import wraps
from ..env import LOG
from ..telemetry.log import bound_logging_vars


def generate_temp_id() -> str:
    return uuid.uuid4().hex


def track_process(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        func_name = func.__name__
        use_id = generate_temp_id()
        with bound_logging_vars(temp_id=use_id, func_name=func_name):
            LOG.info("process.enter", func_name=func_name, temp_id=use_id)
            try:
                return await func(*args, **kwargs)
            finally:
                LOG.info("process.exit", func_name=func_name, temp_id=use_id)

    return wrapper
