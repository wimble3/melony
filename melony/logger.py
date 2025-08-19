from logging import getLogger


_logger = getLogger(__name__)
_MELONY_LOG_PREFIX = "[Melony]"
_DEBUG = True  # @@@


def log_info(message: str) -> None:
    if _DEBUG:
        print(f"{_MELONY_LOG_PREFIX}[INFO] {message}")
    else:
        _logger.info(message)


def log_error(message: str) -> None:
    if _DEBUG:
        print(f"{_MELONY_LOG_PREFIX}[ERROR] {message}")
    else:
        _logger.error(message)
