import asyncio
import time
from redis import Redis as SyncRedis
from redis.asyncio import Redis

from melony import RedisBroker, RedisResultBackend

sync_conn = SyncRedis(host='localhost', port=6379)
sync_broker = RedisBroker(redis_connection=sync_conn)

async_conn = Redis(host='localhost', port=6379)
async_broker = RedisBroker(redis_connection=async_conn, result_backend=RedisResultBackend(redis_connection=async_conn))


@sync_broker.task(queue='notification', retries=2, retry_timeout=30)
def example_task(string_param: str) -> str:
    time.sleep(5)
    return string_param.upper()


@async_broker.task(retries=2, retry_timeout=30)
async def async_example_task(integer: int) -> int:
    await asyncio.sleep(5)
    return integer ** 2
