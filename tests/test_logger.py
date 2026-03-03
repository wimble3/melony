from unittest.mock import patch

from melony.logger import log_error, log_info


def test_log_info_without_consumer_id_prints_message(capsys):
    log_info("hello world")
    assert "hello world" in capsys.readouterr().out


def test_log_info_with_consumer_id_includes_consumer_label(capsys):
    log_info("hello world", consumer_id=3)
    output = capsys.readouterr().out
    assert "consumer-3" in output
    assert "hello world" in output


def test_log_error_prints_message(capsys):
    log_error("something went wrong")
    assert "something went wrong" in capsys.readouterr().out


def test_log_error_prints_exception_traceback(capsys):
    try:
        raise RuntimeError("boom")
    except RuntimeError as exc:
        log_error("caught error", exc=exc)
    assert "boom" in capsys.readouterr().out


def test_log_info_non_debug_calls_logger_without_consumer_id():
    with patch("melony.logger._DEBUG", False):
        with patch("melony.logger._logger") as mock_logger:
            log_info("msg")
            mock_logger.info.assert_called_once()


def test_log_info_non_debug_calls_logger_with_consumer_id():
    with patch("melony.logger._DEBUG", False):
        with patch("melony.logger._logger") as mock_logger:
            log_info("msg", consumer_id=0)
            mock_logger.info.assert_called_once()


def test_log_error_non_debug_calls_logger_without_consumer_id():
    with patch("melony.logger._DEBUG", False):
        with patch("melony.logger._logger") as mock_logger:
            log_error("err")
            mock_logger.error.assert_called_once()


def test_log_error_non_debug_calls_logger_with_consumer_id():
    with patch("melony.logger._DEBUG", False):
        with patch("melony.logger._logger") as mock_logger:
            log_error("err", consumer_id=2)
            mock_logger.error.assert_called_once()
