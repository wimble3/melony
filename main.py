import asyncio

from melony.brokers.mock_broker import MockBroker
from tasks import example_task

# from melony.brokers.redis import RedisBroker

# broker = RedisBroker("redis://localhost:6379/0")



async def main():
    await example_task(number=2, string_param="some str here").delay()
    task = await example_task(number=43434, string_param="5455").delay(countdown=10)
    task_result = await task.get_result()

if __name__ == "__main__":
    asyncio.run(main())
