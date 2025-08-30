import asyncio

from melony.brokers.mock_broker import MockBroker

# from melony.brokers.redis import RedisBroker

# broker = RedisBroker("redis://localhost:6379/0")
broker = MockBroker()

@broker.task
async def example_task(number: int, string_param: str) -> str:
    await asyncio.sleep(1)
    result = number * 2
    return f"Processed: {result}, {string_param}"


async def main():
    await example_task(number=2, string_param="some str here").delay()
    await example_task(number=43434, string_param="5455").delay(countdown=10)


if __name__ == "__main__":
    asyncio.run(main())
