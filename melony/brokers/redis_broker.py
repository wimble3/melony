from redis.asyncio.client import Redis

from melony.core.tasks import Task
from melony.core.brokers import BaseBroker


class RedisBroker(BaseBroker):
    _queue_prefix = "tasks:"

    def __init__(self, connection_str: str) -> None:
        self._connection = Redis.from_url(connection_str)

    async def push(
            self,
            task: Task,
    ) -> None:
        ...
