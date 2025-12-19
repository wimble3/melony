from typing import cast, final, override
from redis import Redis as SyncRedis

from melony.core.consts import REDIS_QUEUE_NAME
from melony.core.publishers import ISyncPublisher
from melony.core.tasks import Task
from melony.core.json_task_converter import SyncJsonTaskConverter


@final
class RedisPublisher(ISyncPublisher):
    def __init__(self, connection: SyncRedis) -> None:
        self._connection = connection
        self._task_converter = SyncJsonTaskConverter()

    @property
    @override
    def connection(self) -> SyncRedis:
        return self._connection

    @override
    def push(self, task: Task) -> None:
        cast(
            int, self._connection.lpush(
                REDIS_QUEUE_NAME,
                self._task_converter.serialize_task(task)
            )
        )
