import time

from melony.sync.brokers.redis_broker import RedisBroker
from melony.sync.result_backends.redis_result_backend import RedisResultBackend


connection_str = "redis://localhost:6379/0"
result_backend = RedisResultBackend(connection_str)
broker = RedisBroker(connection_str, result_backend)


@broker.task
def example_task(number: int, string_param: str) -> str:
    time.sleep(5)
    return f"{number}, {string_param}"