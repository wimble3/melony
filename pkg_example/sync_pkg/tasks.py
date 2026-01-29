import time

from redis import Redis

from melony import RedisBroker, RedisResultBackend


redis_connection = Redis(host="localhost", port=6379, db=0)
result_backend = RedisResultBackend(redis_connection)
broker = RedisBroker(redis_connection, result_backend)


@broker.task
def example_task(number: int, string_param: str) -> str:
    time.sleep(5)
    return f"{number}, {string_param}"


broker.consumer.start_consume()