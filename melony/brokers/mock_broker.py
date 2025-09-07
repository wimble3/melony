import asyncio

from datetime import datetime
from typing import Sequence
from melony.core.brokers import EPSILON, BaseBroker
from melony.core.tasks import Task, TaskJSONSerializable
from melony.logger import log_error, log_info
from melony.task_converters.json_ser_obj_task_converter import JsonSerObjTaskConverter


class MockBroker(BaseBroker, JsonSerObjTaskConverter):
    def __init__(self) -> None:
        self._queue: list[TaskJSONSerializable] = []
        self._task_results: list = []  # @@@ TODO typing

    async def push(
            self,
            task: Task,
    ) -> None:
        serialized_task = self.serialize_task(task)
        self._queue.append(serialized_task)
        log_info(f"Queue (len: {len(self._queue)}): {self._queue}")

    async def pop(self) -> Task | None:
        if not len(self._queue):
            return None
        serialized_task = self._queue.pop(0)
        return self.deserialize_task(serialized_task, broker=self)

    
    async def _wait_epsilon(self) -> None:
        await asyncio.sleep(EPSILON)

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

    async def _execute_appropriate_tasks(self, tasks: Sequence[Task]) -> None:
        tasks_to_execute = await self._filter_tasks_by_execution_time(tasks)
        await self._execute_tasks(tasks=tasks_to_execute)