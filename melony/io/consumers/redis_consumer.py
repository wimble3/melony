from typing import Awaitable, Final, Optional, Sequence, cast, final, override

from melony.core.json_task_converter import AsyncJsonTaskConverter
from melony.core.brokers import BaseBroker
from melony.core.consts import REDIS_QUEUE_NAME
from melony.core.consumers import BaseAsyncConsumer
from melony.core.publishers import IAsyncPublisher
from melony.core.result_backends import IAsyncResultBackend
from melony.core.tasks import Task


@final
class RedisConsumer(BaseAsyncConsumer):
    _brpop_timeout: Final[float] = 0.01

    def __init__(
        self,
        publisher: IAsyncPublisher,
        broker: BaseBroker,
        result_backend: IAsyncResultBackend | None = None
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
