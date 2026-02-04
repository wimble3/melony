import asyncio

from typing import Any, final, Iterable
from redis.asyncio import Redis
from redis import Redis as SyncRedis

from melony.core.dto import TaskResultDTO, TaskExecResultsDTO
from melony.core.result_backends import IAsyncResultBackendSaver, ISyncResultBackendSaver
from melony.core.tasks import Task
from melony.logger import log_error, log_info

__all__ = ()


@final
class AsyncTaskExecutor:
    def __init__(
        self,
        connection: Redis,
        result_backend_saver: IAsyncResultBackendSaver | None = None
    ) -> None:
        self._connection = connection
        self._result_backend = result_backend_saver

    @final
    async def execute_tasks(
        self,
        tasks: Iterable[Task],
        consumer_id: int
    ) -> TaskExecResultsDTO:
        task_map: dict[asyncio.Task, Task] = {}
        asyncio_tasks: list[asyncio.Task] = []
        
        for task in tasks:
            asyncio_task = asyncio.create_task(task.execute())
            task_map[asyncio_task] = task
            asyncio_tasks.append(asyncio_task)
            if task._meta.retries_left:
                task._meta.retries_left -= 1
        
        tasks_exec_results = await self._asyncio_wait_tasks(
            pending=asyncio_tasks,
            task_map=task_map,
            consumer_id=consumer_id
        )
        return tasks_exec_results

    @final
    async def _asyncio_wait_tasks(  # noqa: WPS210, WPS231
        self,
        pending: Iterable[asyncio.Task],
        task_map: dict[asyncio.Task, Task],
        consumer_id: int
    ) -> TaskExecResultsDTO:
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
                        f"Task with id {task.task_id} has been executed with error: "
                        f"{task_result}",
                        exc=task_result,
                        consumer_id=consumer_id
                    )
                    tasks_to_retry.append(task)
                    continue
                log_info(
                    f"Task with id '{task.task_id}' completed with result "
                    f"{task_result=}",
                    consumer_id=consumer_id
                )
                tasks_done.append(TaskResultDTO(task=task, task_result=task_result))
            if self._result_backend:
                await self._result_backend.save_results(task_results=tasks_done)
        return TaskExecResultsDTO(
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


@final
class SyncTaskExecutor:
    def __init__(
        self,
        connection: SyncRedis,
        result_backend_saver: ISyncResultBackendSaver | None = None
    ) -> None:
        self._connection = connection
        self._result_backend_saver = result_backend_saver

    @final
    def execute_tasks(
        self,
        tasks: Iterable[Task], 
        consumer_id: int
    ) -> TaskExecResultsDTO:
        tasks_to_retry: list[Task] = []
        tasks_done: list[TaskResultDTO] = []
        for task in tasks:
            if task._meta.retries_left:
                task._meta.retries_left -= 1
            try:
                task_result = task.execute()
            except Exception as exc:
                task_result = exc

            if isinstance(task_result, BaseException):
                log_error(
                    f"Task with id '{task.task_id}' has been executed with error: "
                    f"{task_result}",
                    exc=task_result
                )
                tasks_to_retry.append(task)
                continue

            log_info(
                f"Task with id '{task.task_id}' completed with result {task_result=}",
                consumer_id=consumer_id
            )
            tasks_done.append(TaskResultDTO(task=task, task_result=task_result))

            if self._result_backend_saver:
                self._result_backend_saver.save_results(task_results=tasks_done)
        
        return TaskExecResultsDTO(
            tasks_with_result=tasks_done,
            tasks_to_retry=tasks_to_retry
        )
