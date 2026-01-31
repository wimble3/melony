from typing import Final

__all__ = ()

REDIS_RESULT_BACKEND_KEY: Final[str] = "melony_result_backend:"

QUEUE_PREFIX: Final[str] = "melony_tasks:"
DEFAULT_QUEUE: Final[str] = f"default"