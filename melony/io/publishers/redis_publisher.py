from typing import Awaitable, cast, final, override
from redis.asyncio import Redis

from melony.core.consts import REDIS_QUEUE_NAME
from melony.core.publishers import IAsyncPublisher
from melony.core.tasks import Task
from melony.core.json_task_converter import AsyncJsonTaskConverter


@final
class RedisPublisher(IAsyncPublisher):
    def __init__(self, connection: Redis) -> None:
        self._connection = connection
        self._task_converter = AsyncJsonTaskConverter()

    @property
    @override
    def connection(self) -> Redis:
        return self._connection

    @override
    async def push(self, task: Task) -> None:
        await cast(
            Awaitable[int], self._connection.lpush(
                REDIS_QUEUE_NAME,
                self._task_converter.serialize_task(task)
            )
        )
