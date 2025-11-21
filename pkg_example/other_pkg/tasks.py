import asyncio
from melony.brokers.redis_broker import RedisBroker


broker = RedisBroker("redis://localhost:6379/0")


@broker.task
async def example_task(number: int, string_param: str) -> str:
    await asyncio.sleep(3)
    raise Exception("Help message for exeption")
    result = number * 2
    return f"Processed: {result}, {string_param}"