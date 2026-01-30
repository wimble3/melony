import asyncio
import multiprocessing

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Sequence, final
from redis.exceptions import ConnectionError

from melony.core.dto import FilteredTasksDTO, TaskExecResultsDTO
from melony.core.publishers import IAsyncPublisher, ISyncPublisher
from melony.core.result_backends import (
    IAsyncResultBackendSaver,
    IResultBackend,
    ISyncResultBackendSaver
)
from melony.core.task_executor import AsyncTaskExecutor, SyncTaskExecutor
from melony.core.tasks import Task
from melony.logger import log_error, log_info


__all__ = ()


class BaseConsumer:
    @final
    def _filter_tasks_by_execution_time(
        self,
        tasks: Sequence[Task],
        consumer_id
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
        
        log_info(f"@@@ {len(tasks_to_execute)=}", consumer_id=consumer_id)
        return FilteredTasksDTO(
            tasks_to_execute=tasks_to_execute,
            tasks_to_push_back=tasks_to_push_back
        )


class BaseAsyncConsumer(ABC, BaseConsumer):  # noqa: WPS214
    def __init__(
        self,
        publisher: IAsyncPublisher,
        result_backend: IResultBackend | None = None
    ) -> None:
        self._publisher = publisher
        self._connection = self._publisher.connection
        self._result_backend = result_backend

        result_backend_saver = result_backend.saver if result_backend else None
        if result_backend_saver:
            assert isinstance(result_backend_saver, IAsyncResultBackendSaver)
        
        self._task_executor = AsyncTaskExecutor(
            connection=self._connection,
            result_backend_saver=result_backend_saver
        )


    @final
    async def start_consume(self, processes: int = 1) -> None:
        if processes < 1:
            raise ValueError("Param 'processes' must be positive integer (without zero)")

        if processes == 1:
            await self._consumer_loop()
            return

        for process_num in range(processes):
            process = multiprocessing.Process(
                name=f"melony-process-{process_num}",
                target=self._run_consumer_in_process,
                args=(process_num,),
                daemon=False
            )
            process.start()

    @abstractmethod
    async def _pop_tasks(self) -> Sequence[Task]:
        ...

    @final
    async def _consumer_loop(self, consumer_id: int = 1) -> None:
        log_info("Start listening...", consumer_id=consumer_id)
        while True:
            try:
                await self._consumer_loop_iteration(consumer_id)
            except ConnectionError as exc:
                log_error(f"Redis connection error", exc=exc)
                break
            except Exception as exc:
                log_error(f"Unexpected error at consuming loop", exc=exc)
                break

    @final
    async def _consumer_loop_iteration(self, consumer_id) -> None:
        tasks = await self._pop_tasks()
        filtered_tasks = self._filter_tasks_by_execution_time(tasks, consumer_id)
        await self._push_bulk(tasks=filtered_tasks.tasks_to_push_back)
        wait_task_results = await self._task_executor.execute_tasks(
            tasks=filtered_tasks.tasks_to_execute
        )
        await self._retry_policy(wait_task_results)

    @final
    async def _push_bulk(self, tasks: Sequence[Task]) -> None:
        push_coroutines = [self._publisher.push(task) for task in tasks]
        await asyncio.gather(*[coro for coro in push_coroutines if coro is not None])

    @final
    async def _retry_policy(
        self,
        tasks_exec_results: TaskExecResultsDTO
    ) -> None:
        tasks_to_push_back: list[Task] = []
        for task_to_retry in tasks_exec_results.tasks_to_retry:
            if task_to_retry._meta.retries_left:
                if task_to_retry.retry_timeout:
                    task_to_retry._meta.timestamp += task_to_retry.retry_timeout
                tasks_to_push_back.append(task_to_retry)

        await self._push_bulk(tasks_to_push_back)

    @final
    def _run_consumer_in_process(
        self,
        consumer_id: int = 0
    ) -> None:
        asyncio.run(self._consumer_loop(consumer_id=consumer_id))


class BaseSyncConsumer(ABC, BaseConsumer):
    def __init__(
        self,
        publisher: ISyncPublisher,
        result_backend: IResultBackend | None = None
    ) -> None:
        self._publisher = publisher
        self._connection = self._publisher.connection
        self._result_backend = result_backend

        result_backend_saver = result_backend.saver if result_backend else None
        if result_backend_saver:
            assert isinstance(result_backend_saver, ISyncResultBackendSaver)

        self._task_executor = SyncTaskExecutor(
            connection=self._connection,
            result_backend_saver=result_backend_saver
        )

    @final
    def start_consume(self, processes: int = 1) -> None:
        if processes < 1:
            raise ValueError("Param 'processes' must be positive integer (without zero)")

        if processes == 1:
            self._consumer_loop()
            return

        for process_num in range(processes):
            process = multiprocessing.Process(
                name=f"melony-process-{process_num}",
                target=self._consumer_loop,
                args=(process_num,),
                daemon=False
            )
            process.start()

    @abstractmethod
    def _pop_tasks(self) -> Sequence[Task]:
        ...

    @final
    def _consumer_loop(self, consumer_id: int = 1) -> None:
        log_info("Start listening...", consumer_id=consumer_id)
        while True:
            try:
                self._consumer_loop_iteration(consumer_id)
            except ConnectionError as exc:
                log_error(f"Redis connection error", exc=exc)
                break
            except Exception as exc:
                log_error(f"Unexpected error at consuming loop", exc=exc)
                break

    @final
    def _consumer_loop_iteration(self, consumer_id) -> None:
        tasks = self._pop_tasks()
        filtered_tasks = self._filter_tasks_by_execution_time(tasks, consumer_id)
        self._push_bulk(tasks=filtered_tasks.tasks_to_push_back)
        wait_task_results = self._task_executor.execute_tasks(
            tasks=filtered_tasks.tasks_to_execute
        )
        self._retry_policy(wait_task_results)

    @final
    def _push_bulk(self, tasks: Sequence[Task]) -> None:
        for task in tasks:
            self._publisher.push(task)

    @final
    def _retry_policy(
        self,
        tasks_exec_results: TaskExecResultsDTO
    ) -> None:
        tasks_to_push_back: list[Task] = []
        for task_to_retry in tasks_exec_results.tasks_to_retry:
            if task_to_retry._meta.retries_left:
                if task_to_retry.retry_timeout:
                    task_to_retry._meta.timestamp += task_to_retry.retry_timeout
                tasks_to_push_back.append(task_to_retry)

        self._push_bulk(tasks_to_push_back)
