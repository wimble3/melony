import asyncio

from typing import Sequence
from typing_extensions import Final
from melony.core.brokers import BaseBroker
from melony.core.tasks import Task, TaskJSONSerializable
from melony.task_converters.json_ser_obj_task_converter import JsonSerObjTaskConverter


class MockBroker(BaseBroker, JsonSerObjTaskConverter):
    _epsilon: Final[float] = 0.0  # noqa: WPS358

    def __init__(self) -> None:
        self._queue: list[TaskJSONSerializable] = []

    @property
    def epsilon(self) -> float:
        return self._epsilon
    
    async def push(
        self,
        task: Task,
    ) -> None:
        serialized_task = self.serialize_task(task)
        self._queue.append(serialized_task)

    async def pop(self) -> Task | None:
        if not len(self._queue):
            return None
        serialized_task = self._queue.pop(0)
        return self.deserialize_task(serialized_task, broker=self)

    async def _pop_tasks(self) -> Sequence[Task]:
        if not self._queue:
            return []
        
        pop_coroutines = [self.pop() for _ in range(len(self._queue))]
        pop_results = await asyncio.gather(*pop_coroutines, return_exceptions=True)
        
        tasks: list[Task] = []
        for task in pop_results:
            if task is None:
                break
            elif isinstance(task, BaseException):
                break
            tasks.append(task)
        return tasks
