"""CLI module for melony task queue."""

import argparse
import asyncio
import sys
from typing import Optional

from melony.core.brokers import BaseBroker


def create_parser() -> argparse.ArgumentParser:
    """Create argument parser for melony CLI."""
    parser = argparse.ArgumentParser(
        prog="melony",
        description="Melony task queue CLI"
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Consumer command
    consumer_parser = subparsers.add_parser("consumer", help="Start task consumer")
    consumer_parser.add_argument(
        "--queue",
        default="default",
        help="Queue name to consume from (default: default)"
    )
    consumer_parser.add_argument(
        "--processes",
        type=int,
        default=1,
        help="Number of processes (default: 1)"
    )
    consumer_parser.add_argument(
        "--sync",
        action="store_true",
        help="Use sync consumer instead of async"
    )

    # Cron consumer command
    cron_parser = subparsers.add_parser("cron", help="Start cron consumer")
    cron_parser.add_argument(
        "--queue",
        default="default",
        help="Queue name to consume from (default: default)"
    )
    cron_parser.add_argument(
        "--processes",
        type=int,
        default=1,
        help="Number of processes (default: 1)"
    )

    return parser


async def run_consumer(broker: BaseBroker, queue: str, processes: int) -> None:
    """Run async consumer."""
    await broker.consumer.start_consume(queue=queue, processes=processes)


def run_sync_consumer(broker: BaseBroker, queue: str, processes: int) -> None:
    """Run sync consumer."""
    broker.consumer.start_consume(queue=queue, processes=processes)


async def run_cron_consumer(broker: BaseBroker, queue: str, processes: int) -> None:
    """Run cron consumer."""
    await broker.cron_consumer.start_consume(queue=queue, processes=processes)


def main(broker: BaseBroker, argv: Optional[list[str]] = None) -> None:
    """Main CLI entry point."""
    parser = create_parser()

    try:
        args = parser.parse_args(argv)
    except SystemExit as e:
        if e.code == 2:
            sys.exit(1)
        else:
            raise

    if args.command is None:
        parser.print_help()
        sys.exit(1)

    try:
        if args.command == "consumer":
            if args.sync:
                run_sync_consumer(broker, args.queue, args.processes)
            else:
                asyncio.run(run_consumer(broker, args.queue, args.processes))
        elif args.command == "cron":
            asyncio.run(run_cron_consumer(broker, args.queue, args.processes))
        else:
            parser.print_help()
            sys.exit(1)
    except KeyboardInterrupt:
        print("\nStopped by user")
        sys.exit(0)
    except Exception as e:
        import logging
        logging.error("Error occurred", exc_info=True)
        sys.exit(1)