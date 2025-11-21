import asyncio

from abc import ABC, abstractmethod
from datetime import datetime
from functools import wraps
from inspect import signature
from typing import Any, Callable, ParamSpec, Sequence, TypeVar, final

from melony.core.dto import FilteredTasksDTO
from melony.core.tasks import TaskWrapper, Task
from melony.logger import log_error, log_info

_TaskParams = ParamSpec("_TaskParams")
_TaskResult = TypeVar("_TaskResult")


class BaseBroker(ABC):  # noqa: WPS214
    @property
    @abstractmethod
    def epsilon(self) -> float:
        ...

    @abstractmethod
    async def push(self, task: Task) -> None:
        ...

    @final
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

    @final
    async def start_consume(self) -> None:
        while True:
            try:
                await self._consumer_loop()
            except Exception as exc:
                log_error(f"Unexpected error at consuming process", exc=exc)

    @abstractmethod
    async def _pop_tasks(self) -> Sequence[Task]:
        ...

    @final
    async def _consumer_loop(self) -> None:
        await asyncio.sleep(self.epsilon)
        tasks = await self._pop_tasks()
        filtered_tasks = await self._filter_tasks_by_execution_time(tasks)
        await self._push_bulk(tasks=filtered_tasks.tasks_to_push_back)
        await self._execute_tasks(tasks=filtered_tasks.tasks_to_execute)

    @final
    async def _push_bulk(self, tasks: Sequence[Task]) -> None:
        push_coroutines = [self.push(task) for task in tasks]
        await asyncio.gather(*push_coroutines)

    @final
    async def _execute_tasks(self, tasks: Sequence[Task]) -> None:
        if not tasks:
            return

        task_map: dict[asyncio.Task, Task] = {}
        asyncio_tasks: list[asyncio.Task] = []
        
        for task in tasks:
            asyncio_task = asyncio.create_task(task.execute())
            task_map[asyncio_task] = task
            asyncio_tasks.append(asyncio_task)
        
        pending = set(asyncio_tasks)
        await self._asyncio_wait_tasks(pending, task_map)
        
    @final
    async def _asyncio_wait_tasks(
        self,
        pending: set[asyncio.Task],
        task_map: dict[asyncio.Task, Task]
    ) -> None:
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
                    # TODO: retry right here
                    continue
                log_info(
                    f"Task with id '{task.task_id}' completed with result "
                    f"{task_result=}"
                )

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
    async def _retry_policy(self) -> None:
        ...

    @final
    def _get_task_result(self, asyncio_task: asyncio.Task) -> Any:  # TODO: typing
        task_exc = asyncio_task.exception()
        if task_exc:
            return task_exc
        task_result = asyncio_task.result()
        return task_result