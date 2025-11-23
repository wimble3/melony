from typing import Awaitable, cast, override
from redis.asyncio import Redis

from melony.core.consts import REDIS_QUEUE_NAME
from melony.core.publishers import IPublisher
from melony.core.tasks import Task
from melony.core.json_task_converter import JsonTaskConverter


class RedisPublisher(IPublisher):
    def __init__(self, connection: Redis) -> None:
        self._connection = connection
        self._task_converter = JsonTaskConverter()

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
