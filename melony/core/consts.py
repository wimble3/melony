from typing import Final

__all__ = ()

# default
QUEUE_PREFIX: Final[str] = "melony_tasks:"
CRON_QUEUE_PREFIX: Final[str] = "melony_cron:"
DEFAULT_QUEUE: Final[str] = f"default"

# redis
REDIS_RESULT_BACKEND_KEY: Final[str] = "melony_result_backend:"
