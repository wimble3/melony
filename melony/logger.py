import traceback

from datetime import datetime
from logging import getLogger
from typing import Final


_logger = getLogger(__name__)
_MELONY_LOG_PREFIX: Final[str] = "[Melony]"
_DEBUG = True  # Toggle this for debugging via print python function  TODO: to env
_WRAP_LINE_WIDTH: Final[int] = 60


def log_info(message: str) -> None:
    if _DEBUG:
        print(f"{_MELONY_LOG_PREFIX}[INFO][{datetime.now()}] {message}")  # noqa: WPS421
    else:
        _logger.info(message)


def log_error(message: str, exc: BaseException | None = None) -> None:
    if _DEBUG:
        print(f"{_MELONY_LOG_PREFIX}[ERROR][{datetime.now()}] {message}")  # noqa: WPS421
        if exc:
            formatted_traceback = traceback.format_exception(exc)
            for line in formatted_traceback:
                print(line)  # noqa: WPS421
            print("-" * _WRAP_LINE_WIDTH)  # noqa: WPS421
    else:
        _logger.error(message, exc_info=True)
