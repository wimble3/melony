import asyncio

import click

from melony.core.consts import DEFAULT_QUEUE
from melony.core.finders import find_broker

__all__ = ()


@click.group()
def cli() -> None:
    """Melony CLI."""


@cli.command()
@click.option("--broker", "-b", required=True, help="Path to broker instance, e.g. src.tasks.broker")
@click.option("--queue", "-q", default=DEFAULT_QUEUE, show_default=True, help="Queue name to consume from")
@click.option("--processes", "-p", default=1, show_default=True, help="Number of worker processes")
def start_consume(broker: str, queue: str, processes: int) -> None:
    """Start consuming tasks from the queue."""
    broker_instance = find_broker(broker)
    consumer = broker_instance.consumer
    asyncio.run(consumer.start_consume(queue=queue, processes=processes))


@cli.command()
@click.option("--broker", "-b", required=True, help="Path to broker instance, e.g. src.tasks.broker")
@click.option("--queue", "-q", default=DEFAULT_QUEUE, show_default=True, help="Queue name to consume from")
@click.option("--processes", "-p", default=1, show_default=True, help="Number of worker processes")
def start_cron_consume(broker: str, queue: str, processes: int) -> None:
    """Start consuming cron tasks."""
    broker_instance = find_broker(broker)
    cron_consumer = broker_instance.cron_consumer
    asyncio.run(cron_consumer.start_consume(queue=queue, processes=processes))