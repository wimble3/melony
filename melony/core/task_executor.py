import asyncio

from collections.abc import Iterable
from typing import Any, Sequence, final
from redis.asyncio import Redis

from melony.core.dto import TaskResultDTO, WaitTaskResultsDTO
from melony.core.publishers import IPublisher
from melony.core.result_backend import IResultBackend
from melony.core.tasks import Task
from melony.logger import log_error, log_info


@final
class TaskExecutor:
    def __init__(
        self,
        connection: Redis,
        result_backend: IResultBackend | None = None
    ) -> None:
        self._connection = connection
        self._result_backend = result_backend

    @final
    async def execute_tasks(self, tasks: Sequence[Task]) -> WaitTaskResultsDTO:
        task_map: dict[asyncio.Task, Task] = {}
        asyncio_tasks: list[asyncio.Task] = []
        
        for task in tasks:
            asyncio_task = asyncio.create_task(task.execute())
            task_map[asyncio_task] = task
            asyncio_tasks.append(asyncio_task)
            if task._meta.retries_left:
                task._meta.retries_left -= 1
        
        wait_tasks_results = await self._asyncio_wait_tasks(
            pending=asyncio_tasks,
            task_map=task_map
        )
        return wait_tasks_results

    @final
    async def _asyncio_wait_tasks(  # noqa: WPS210
        self,
        pending: Iterable[asyncio.Task],
        task_map: dict[asyncio.Task, Task]
    ) -> WaitTaskResultsDTO:
        tasks_to_retry: list[Task] = []
        tasks_done: list[TaskResultDTO] = []
        while pending:
            done, pending = await asyncio.wait(
                pending, return_when=asyncio.FIRST_COMPLETED
            )
            for completed_asyncio_task in done:
                task = task_map[completed_asyncio_task]
                task_result = self._get_task_result(asyncio_task=completed_asyncio_task)
                if isinstance(task_result, BaseException):
                    log_error(
                        f"Task with id '{task.task_id}' has been executed with error: "
                        f"{task_result}",
                        exc=task_result
                    )
                    tasks_to_retry.append(task)
                    continue
                log_info(
                    f"Task with id '{task.task_id}' completed with result {task_result=}"
                )
                tasks_done.append(TaskResultDTO(task=task, task_result=task_result))
            if self._result_backend:
                await self._result_backend.save_results(task_results=tasks_done)
        return WaitTaskResultsDTO(
            tasks_with_result=tasks_done,
            tasks_to_retry=tasks_to_retry
        )

    @final
    def _get_task_result(self, asyncio_task: asyncio.Task) -> Any:  # TODO: typing
        task_exc = asyncio_task.exception()
        if task_exc:
            return task_exc
        task_result = asyncio_task.result()
        return task_result
