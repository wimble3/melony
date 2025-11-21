from typing import Final, Sequence, Awaitable, cast, Optional
from redis.asyncio.client import Redis

from melony.core.tasks import Task
from melony.core.brokers import BaseBroker
from melony.logger import log_info
from melony.task_converters.json_task_converter import JsonTaskConverter


class RedisBroker(BaseBroker, JsonTaskConverter):
    _queue_name: Final[str] = "melony_tasks"
    _epsilon: Final[float] = 0.05
    _brpop_timeout: Final[float] = 0.01

    def __init__(self, connection_str: str) -> None:
        self._connection = Redis.from_url(connection_str)

    @property
    def epsilon(self) -> float:
        return self._epsilon

    async def push(self, task: Task) -> None:
        await cast(
            Awaitable[int], self._connection.lpush(
                self._queue_name,
                self.serialize_task(task)
            )
        )

    async def _pop_tasks(self) -> Sequence[Task]:
        tasks: list[Task] = []
        redis_task = await cast(
            Awaitable[Sequence[bytes]],
            self._connection.brpop(keys=[self._queue_name])
        )
        tasks.append(self._get_task_from_redis_task(redis_task))
        
        # TODO: batching
        while True:
            redis_task = await cast(
                Awaitable[Optional[Sequence[bytes]]],
                self._connection.brpop(
                    keys=[self._queue_name],
                    timeout=self._brpop_timeout
                )
            )
            if not redis_task:
                break
            tasks.append(self._get_task_from_redis_task(redis_task))
        
        return tasks

    def _get_task_from_redis_task(self, redis_task: Sequence[bytes]) -> Task:
        serialized_task_bytes = redis_task[1]
        serialized_task = serialized_task_bytes.decode("utf-8")
        task = self.deserialize_task(
            serialized_task=serialized_task,
            broker=self
        )
        return task
        