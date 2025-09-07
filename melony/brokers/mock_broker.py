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
        tasks: list[Task] = []
        for _ in range(len(self._queue)):
            task = await self.pop()
            if not task:
                raise RuntimeError(f"Pop by time at loop returns None, not Task object")
            tasks.append(task)
        return tasks
            
    async def _execute_appropriate_tasks(self, tasks: Sequence[Task]) -> None:
        tasks_to_execute = await self._filter_tasks_by_execution_time(tasks)
        await self._execute_tasks(tasks=tasks_to_execute)

    async def _filter_tasks_by_execution_time(self, tasks: Sequence[Task]) -> Sequence[Task]:
        tasks_to_execute: list[Task] = []
        for task in tasks:
            task_execution_timestamp = task.get_execution_timestamp()
            if task_execution_timestamp < datetime.timestamp(datetime.now()):
                tasks_to_execute.append(task)
            else:
                await self.push(task)

        return tasks_to_execute

    async def _execute_tasks(self, tasks: Sequence[Task]) -> None:
        task_map = {}
        asyncio_tasks = []
        
        for task in tasks:
            asyncio_task = asyncio.create_task(task.execute())
            task_map[asyncio_task] = task
            asyncio_tasks.append(asyncio_task)
        
        pending = set(asyncio_tasks)
        
        while pending:
            done, pending = await asyncio.wait(
                pending, return_when=asyncio.FIRST_COMPLETED
            )
            for completed_asyncio_task in done:
                task = task_map[completed_asyncio_task]
                try:
                    result = await completed_asyncio_task
                    log_info(f"Task '{task}' completed: {result}")
                except Exception as e:
                    log_error(
                        f"Unexpected error while task {task} execution: {e}"
                    )

        

