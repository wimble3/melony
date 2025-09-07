from abc import ABC, abstractmethod
import asyncio
from datetime import datetime
from functools import wraps
from inspect import signature
from typing import Callable, Final, ParamSpec, Sequence, TypeVar

from melony.core.tasks import TaskWrapper, Task
from melony.logger import log_error, log_info

_TaskParams = ParamSpec("_TaskParams")
_TaskResult = TypeVar("_TaskResult")
EPSILON: Final = 0.05



class BaseBroker(ABC):
    @abstractmethod
    async def push(self, task: Task) -> None:
        ...

    def task(
            self,
            func: Callable[_TaskParams, _TaskResult]
    ) -> Callable[_TaskParams, TaskWrapper]:
        @wraps(func)
        def wrapper(
                *args: _TaskParams.args,
                **kwargs: _TaskParams.kwargs
        ) -> TaskWrapper:
            sig = signature(func)
            bound = sig.bind(*args, **kwargs)
            bound.apply_defaults()

            return TaskWrapper(
                func=func,
                broker=self,
                bound_args=bound.arguments
            )

        wrapper.__annotations__ = {
            **func.__annotations__,
            "return": TaskWrapper
        }
        return wrapper

    async def start_consume(self) -> None:
        start_time = datetime.now()
        log_info("Consumer starts")
        while True:
            try:
                await self._consumer_loop(start_time)
            except Exception as exc:
                log_error(f"Unexpected error at consuming process: {exc}")

    async def _consumer_loop(self, start_time) -> None:
        log_info(f"time spent: {datetime.now() - start_time}")
        await self._wait_epsilon()
        tasks = await self._pop_tasks()
        await self._execute_appropriate_tasks(tasks)

    @abstractmethod
    async def _wait_epsilon(self) -> None:
        ...

    @abstractmethod
    async def _pop_tasks(self) -> Sequence[Task]:
        ...

    @abstractmethod
    async def _execute_appropriate_tasks(self, tasks: Sequence[Task]) -> None:
        ...

    async def _filter_tasks_by_execution_time(
            self,
            tasks: Sequence[Task]
    ) -> Sequence[Task]:
        tasks_to_execute: list[Task] = []
        for task in tasks:
            task_execution_timestamp = task.get_execution_timestamp()
            if task_execution_timestamp < datetime.timestamp(datetime.now()):
                tasks_to_execute.append(task)
            else:
                await self.push(task)

        return tasks_to_execute

    async def _execute_tasks(self, tasks: Sequence[Task]) -> None:
        task_map: dict[asyncio.Task, Task] = {}
        asyncio_tasks: list[asyncio.Task] = []
        
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
                except Exception as exc:
                    log_error(
                        f"Unexpected error while task {task} execution: {exc}"
                    )
                log_info(f"Task '{task.task_id}' completed")
