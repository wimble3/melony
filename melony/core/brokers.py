from abc import ABC, abstractmethod
from functools import wraps
from inspect import signature
from typing import Callable, ParamSpec, TypeVar

from core.tasks import Task, TaskWrapper

TaskParams = ParamSpec("TaskParams")
TaskResult = TypeVar("TaskResult")


class BaseBroker(ABC):
    @abstractmethod
    async def push(self, task: Task, countdown: int = 0) -> None:
        ...

    def task(
            self,
            func: Callable[TaskParams, TaskResult]
    ) -> Callable[TaskParams, TaskWrapper[TaskParams, TaskResult]]:
        @wraps(func)
        def wrapper(
                *args: TaskParams.args,
                **kwargs: TaskParams.kwargs
        ) -> TaskWrapper[TaskParams, TaskResult]:
            sig = signature(func)
            bound = sig.bind(*args, **kwargs)
            bound.apply_defaults()
            
            return TaskWrapper[TaskParams, TaskResult](
                func=func,
                broker=self,
                bound_args=bound.arguments
            )
        
        wrapper.__annotations__ = {**func.__annotations__, "return": TaskWrapper[TaskParams, TaskResult]}
        return wrapper
