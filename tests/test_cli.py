"""Tests for CLI module."""

import argparse
import pytest
from unittest.mock import AsyncMock, Mock, patch

from melony.core.cli import create_parser, main, run_consumer, run_sync_consumer, run_cron_consumer


class TestCreateParser:
    """Test argument parser creation."""

    def test_create_parser_returns_argument_parser(self):
        """Test that create_parser returns an ArgumentParser."""
        parser = create_parser()
        assert isinstance(parser, argparse.ArgumentParser)

    def test_parser_has_correct_prog_name(self):
        """Test parser has correct program name."""
        parser = create_parser()
        assert parser.prog == "melony"

    def test_parser_has_subcommands(self):
        """Test parser has consumer and cron subcommands."""
        parser = create_parser()
        
        # Test consumer command
        args = parser.parse_args(["consumer"])
        assert args.command == "consumer"
        assert args.queue == "default"
        assert args.processes == 1
        assert args.sync is False

        # Test consumer with options
        args = parser.parse_args(["consumer", "--queue", "test", "--processes", "3", "--sync"])
        assert args.command == "consumer"
        assert args.queue == "test"
        assert args.processes == 3
        assert args.sync is True

        # Test cron command
        args = parser.parse_args(["cron"])
        assert args.command == "cron"
        assert args.queue == "default"
        assert args.processes == 1

        # Test cron with options
        args = parser.parse_args(["cron", "--queue", "cron_test", "--processes", "2"])
        assert args.command == "cron"
        assert args.queue == "cron_test"
        assert args.processes == 2


class TestRunnerFunctions:
    """Test CLI runner functions."""

    @pytest.mark.asyncio
    async def test_run_consumer(self):
        """Test async consumer runner."""
        mock_broker = Mock()
        mock_broker.consumer.start_consume = AsyncMock()

        await run_consumer(mock_broker, "test_queue", 2)

        mock_broker.consumer.start_consume.assert_called_once_with(
            queue="test_queue", processes=2
        )

    def test_run_sync_consumer(self):
        """Test sync consumer runner."""
        mock_broker = Mock()
        mock_broker.consumer.start_consume = Mock()

        run_sync_consumer(mock_broker, "test_queue", 3)

        mock_broker.consumer.start_consume.assert_called_once_with(
            queue="test_queue", processes=3
        )

    @pytest.mark.asyncio
    async def test_run_cron_consumer(self):
        """Test cron consumer runner."""
        mock_broker = Mock()
        mock_broker.cron_consumer.start_consume = AsyncMock()

        await run_cron_consumer(mock_broker, "cron_queue", 1)

        mock_broker.cron_consumer.start_consume.assert_called_once_with(
            queue="cron_queue", processes=1
        )


class TestMain:
    """Test main CLI entry point."""

    def test_main_no_command_shows_help(self, capsys):
        """Test main with no command shows help and exits."""
        mock_broker = Mock()

        with pytest.raises(SystemExit) as exc_info:
            main(mock_broker, [])
        
        assert exc_info.value.code == 1
        captured = capsys.readouterr()
        assert "usage:" in captured.out

    @patch('melony.core.cli.asyncio.run')
    def test_main_consumer_command(self, mock_asyncio_run):
        """Test main with consumer command."""
        mock_broker = Mock()

        main(mock_broker, ["consumer", "--queue", "test", "--processes", "2"])

        mock_asyncio_run.assert_called_once()

    @patch('melony.core.cli.run_sync_consumer')
    def test_main_sync_consumer_command(self, mock_sync_runner):
        """Test main with sync consumer command."""
        mock_broker = Mock()

        main(mock_broker, ["consumer", "--sync", "--queue", "test", "--processes", "2"])

        mock_sync_runner.assert_called_once_with(mock_broker, "test", 2)

    @patch('melony.core.cli.asyncio.run')
    def test_main_cron_command(self, mock_asyncio_run):
        """Test main with cron command."""
        mock_broker = Mock()

        main(mock_broker, ["cron", "--queue", "cron_test", "--processes", "1"])

        mock_asyncio_run.assert_called_once()

    def test_main_invalid_command_shows_help(self, capsys):
        """Test main with invalid command shows help and exits."""
        mock_broker = Mock()

        with pytest.raises(SystemExit) as exc_info:
            main(mock_broker, ["invalid"])
        
        assert exc_info.value.code == 1

    @patch('melony.core.cli.asyncio.run')
    def test_main_keyboard_interrupt(self, mock_asyncio_run, capsys):
        """Test main handles KeyboardInterrupt gracefully."""
        mock_asyncio_run.side_effect = KeyboardInterrupt()
        mock_broker = Mock()

        with pytest.raises(SystemExit) as exc_info:
            main(mock_broker, ["consumer"])
        
        assert exc_info.value.code == 0
        captured = capsys.readouterr()
        assert "Stopped by user" in captured.out

    @patch('melony.core.cli.asyncio.run')
    def test_main_exception_handling(self, mock_asyncio_run, capsys):
        """Test main handles exceptions gracefully."""
        mock_asyncio_run.side_effect = Exception("Test error")
        mock_broker = Mock()

        with pytest.raises(SystemExit) as exc_info:
            main(mock_broker, ["consumer"])
        
        assert exc_info.value.code == 1
        captured = capsys.readouterr()
        assert "Error: Test error" in captured.out