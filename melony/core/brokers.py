from abc import ABC, abstractmethod
from functools import wraps
from inspect import signature
from typing import Callable, ParamSpec, TypeVar

from melony.core.tasks import TaskWrapper, Task

TaskParams = ParamSpec("TaskParams")
TaskResult = TypeVar("TaskResult")


class BaseBroker(ABC):
    @abstractmethod
    async def push(self, task: Task) -> None:
        ...

    def task(
            self,
            func: Callable[TaskParams, TaskResult]
    ) -> Callable[TaskParams, TaskWrapper]:
        @wraps(func)
        def wrapper(
                *args: TaskParams.args,
                **kwargs: TaskParams.kwargs
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
