import traceback

from datetime import datetime
from logging import getLogger
from typing import Final


__all__ = ("log_info", "log_error")


_logger = getLogger(__name__)
_MELONY_LOG_PREFIX: Final[str] = "[Melony]"
_DEBUG = True  # Toggle this for debugging via print python function  TODO: to settings
_WRAP_LINE_WIDTH: Final[int] = 60


def log_info(message: str, consumer_id: int | None = None) -> None:
    if _DEBUG:
        if consumer_id is None:
            print(f"{_MELONY_LOG_PREFIX}[INFO][{datetime.now()}]: {message}")  # noqa: WPS421, WPS226
        else:
            print(  # noqa: WPS421
                f"{_MELONY_LOG_PREFIX}[INFO][consumer-{consumer_id}][{datetime.now()}]: "  # noqa: WPS226
                f"{message}"
            )
    else:
        if consumer_id is None:
            _logger.info(f"{_MELONY_LOG_PREFIX}[INFO][{datetime.now()}]: {message}")
        else:
            _logger.info(
                f"{_MELONY_LOG_PREFIX}[INFO][consumer-{consumer_id}][{datetime.now()}]: {message}"
            )



def log_error(
    message: str,
    exc: BaseException | None = None,
    consumer_id: int | None = None
) -> None:
    if _DEBUG:
        print(f"{_MELONY_LOG_PREFIX}[ERROR][{datetime.now()}]: {message}")  # noqa: WPS421
        if exc:
            formatted_traceback = traceback.format_exception(exc)
            for line in formatted_traceback:
                print(line)  # noqa: WPS421
            print("-" * _WRAP_LINE_WIDTH)  # noqa: WPS421
    else:
        if consumer_id is None:
            _logger.error(
                f"{_MELONY_LOG_PREFIX}[ERROR][{datetime.now()}]: {message}", 
                exc_info=True
            )
        else:
            _logger.error(
                f"{_MELONY_LOG_PREFIX}[ERROR][consumer-{consumer_id}]"
                f"[{datetime.now()}]: {message}", 
                exc_info=True
            )
