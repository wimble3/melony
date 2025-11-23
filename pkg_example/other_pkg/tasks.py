import asyncio

from melony.brokers.redis_broker import RedisBroker
from melony.result_backends.redis_result_backend import RedisResultBackend


connection_str = "redis://localhost:6379/0"
result_backend = RedisResultBackend(connection_str)
broker = RedisBroker(connection_str, result_backend)


@broker.task
async def example_task(number: int, string_param: str) -> str:
    await asyncio.sleep(2)
    # raise Exception("Help message for exeption")
    result = number * 2
    return f"Processed: {result}, {string_param}"