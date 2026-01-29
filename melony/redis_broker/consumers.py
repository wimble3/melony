from typing import Awaitable, Final, Optional, Sequence, cast, final, override

from melony.core.task_converters import AsyncJsonTaskConverter, SyncJsonTaskConverter
from melony.core.brokers import BaseBroker
from melony.core.consts import REDIS_QUEUE_NAME
from melony.core.consumers import BaseAsyncConsumer, BaseSyncConsumer
from melony.core.publishers import IAsyncPublisher, ISyncPublisher
from melony.core.result_backends import IResultBackend
from melony.core.tasks import Task

__all__ = ()

type RedisConsumer = AsyncRedisConsumer | SyncRedisConsumer


@final
class AsyncRedisConsumer(BaseAsyncConsumer):
    _brpop_timeout: Final[float] = 0.01

    def __init__(
        self,
        publisher: IAsyncPublisher,
        broker: BaseBroker,
        result_backend: IResultBackend | None = None
    ) -> None:
        super().__init__(publisher, result_backend)
        self._broker = broker
        self._task_converter = AsyncJsonTaskConverter()

    @override
    async def _pop_tasks(self) -> Sequence[Task]:
        tasks: list[Task] = []
        redis_task = await cast(
            Awaitable[Sequence[bytes]],
            self._connection.brpop(keys=[REDIS_QUEUE_NAME])
        )
        tasks.append(self._deserialize_to_task_from_redis(redis_task))
        
        while redis_task:
            redis_task = await cast(
                Awaitable[Optional[Sequence[bytes]]],
                self._connection.brpop(
                    keys=[REDIS_QUEUE_NAME],
                    timeout=self._brpop_timeout
                )
            )
            if redis_task:
                tasks.append(self._deserialize_to_task_from_redis(redis_task))
        
        return tasks

    def _deserialize_to_task_from_redis(self, redis_task: Sequence[bytes]) -> Task:
        serialized_task_bytes = redis_task[1]
        serialized_task = serialized_task_bytes.decode("utf-8")
        task = self._task_converter.deserialize_task(
            serialized_task=serialized_task,
            broker=self._broker
        )
        return task


@final
class SyncRedisConsumer(BaseSyncConsumer):
    _brpop_timeout: Final[float] = 0.01

    def __init__(
        self,
        publisher: ISyncPublisher,
        broker: BaseBroker,
        result_backend: IResultBackend | None = None
    ) -> None:
        super().__init__(publisher, result_backend)
        self._broker = broker
        self._task_converter = SyncJsonTaskConverter()

    @override
    def _pop_tasks(self) -> Sequence[Task]:
        tasks: list[Task] = []
        redis_task = cast(
            Sequence[bytes],
            self._connection.brpop(keys=[REDIS_QUEUE_NAME])
        )
        tasks.append(self._deserialize_to_task_from_redis(redis_task))
        
        while redis_task:
            redis_task = cast(
                Optional[Sequence[bytes]],
                self._connection.brpop(
                    keys=[REDIS_QUEUE_NAME],
                    timeout=self._brpop_timeout
                )
            )
            if redis_task:
                tasks.append(self._deserialize_to_task_from_redis(redis_task))
        
        return tasks

    def _deserialize_to_task_from_redis(self, redis_task: Sequence[bytes]) -> Task:
        serialized_task_bytes = redis_task[1]
        serialized_task = serialized_task_bytes.decode("utf-8")
        task = self._task_converter.deserialize_task(
            serialized_task=serialized_task,
            broker=self._broker
        )
        return task