from abc import ABC, abstractmethod
from functools import wraps
from inspect import signature
from typing import Callable, ParamSpec, TypeVar

from core.tasks import Task, TaskWrapper

P = ParamSpec("P")
R = TypeVar("R")


class BaseBroker(ABC):
    @abstractmethod
    async def push(self, task: Task, countdown: int = 0) -> None:
        ...

    def task(self, func: Callable[P, R]) -> Callable[P, TaskWrapper[P, R]]:
        @wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> TaskWrapper[P, R]:
            sig = signature(func)
            bound = sig.bind(*args, **kwargs)
            bound.apply_defaults()
            
            return TaskWrapper[P, R](
                func=func,
                broker=self,
                bound_args=bound.arguments
            )
        
        # Copy the original function's type hints to the wrapper
        wrapper.__annotations__ = {**func.__annotations__, 'return': TaskWrapper[P, R]}
        return wrapper
