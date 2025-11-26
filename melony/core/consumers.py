import asyncio

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Sequence, final
from redis.exceptions import ConnectionError

from melony.core.dto import FilteredTasksDTO, WaitTaskResultsDTO
from melony.core.publishers import IPublisher
from melony.core.result_backend import IResultBackend
from melony.core.task_executor import TaskExecutor
from melony.core.tasks import Task
from melony.logger import log_error, log_info


class BaseConsumer(ABC):
    def __init__(
        self,
        publisher: IPublisher,
        result_backend: IResultBackend | None = None
    ) -> None:
        self._publisher = publisher
        self._connection = self._publisher.connection
        self._task_executor = TaskExecutor(
            connection=self._connection,
            result_backend=result_backend
        )
        self._result_backend = result_backend

    @final
    async def start_consume(self) -> None:
        # TODO: processes parameter
        log_info("Start listening...")
        while True:
            try:
                await self._consumer_loop()
            except ConnectionError as exc:
                log_error(f"Redis connection error", exc=exc)
                break
            except Exception as exc:
                log_error(f"Unexpected error at consuming loop", exc=exc)

    @abstractmethod
    async def _pop_tasks(self) -> Sequence[Task]:
        ...

    @final
    async def _consumer_loop(self) -> None:
        tasks = await self._pop_tasks()
        filtered_tasks = await self._filter_tasks_by_execution_time(tasks)
        await self._push_bulk(tasks=filtered_tasks.tasks_to_push_back)
        wait_task_results = await self._task_executor.execute_tasks(
            tasks=filtered_tasks.tasks_to_execute
        )
        await self._retry_policy(wait_task_results)

    @final
    async def _push_bulk(self, tasks: Sequence[Task]) -> None:
        push_coroutines = [self._publisher.push(task) for task in tasks]
        await asyncio.gather(*push_coroutines)

    @final
    async def _filter_tasks_by_execution_time(
        self,
        tasks: Sequence[Task]
    ) -> FilteredTasksDTO:
        tasks_to_execute: list[Task] = []
        tasks_to_push_back: list[Task] = []
        current_timestamp = datetime.timestamp(datetime.now())
        
        for task in tasks:
            task_execution_timestamp = task.get_execution_timestamp()
            if task_execution_timestamp < current_timestamp:
                tasks_to_execute.append(task)
            else:
                tasks_to_push_back.append(task)
        
        log_info(f"@@@ {len(tasks_to_execute)=}")
        return FilteredTasksDTO(
            tasks_to_execute=tasks_to_execute,
            tasks_to_push_back=tasks_to_push_back
        )

    @final
    async def _retry_policy(
        self,
        wait_tasks_results: WaitTaskResultsDTO
    ) -> None:
        tasks_to_push_back: list[Task] = []
        for task_to_retry in wait_tasks_results.tasks_to_retry:
            if task_to_retry._meta.retries_left:
                tasks_to_push_back.append(task_to_retry)

        await self._push_bulk(tasks_to_push_back)
