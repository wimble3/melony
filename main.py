import asyncio

from melony.brokers.mock_broker import MockBroker
from melony.core.consumers import Consumer
from pkg_example.other_pkg.tasks import example_task

# from melony.brokers.redis import RedisBroker

# broker = RedisBroker("redis://localhost:6379/0")



async def main():
    await example_task(number=2, string_param="some str here").delay()

    consumer = Consumer(broker=MockBroker())
    await consumer.start_consume()

if __name__ == "__main__":
    asyncio.run(main())
