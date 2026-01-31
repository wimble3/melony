import time
from melony import RedisBroker
from redis import Redis

broker = RedisBroker(redis_connection=Redis(host='localhost', port=6379))

@broker.task(retries=2, retry_timeout=30)
def example_task(string_param: str) -> str:
    time.sleep(5)
    return string_param.upper()


@broker.task(queue='notification', retries=2, retry_timeout=30)
def example_task_notification(integer: int) -> int:
    time.sleep(1)
    return integer ** 2