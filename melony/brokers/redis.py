from redis.asyncio.client import Redis

from core.tasks import TaskWrapper
from melony.core.brokers import BaseBroker


class RedisBroker(BaseBroker):
    _QUEUE_PREFIX = "tasks:"

    def __init__(self, connection_str: str) -> None:
        self._connection = Redis.from_url(connection_str)

    async def push(self, task_wrapper: TaskWrapper, countdown: int = 0) -> None:
        ...
