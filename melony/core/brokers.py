from abc import ABC, abstractmethod
from functools import wraps
from inspect import signature
from typing import Callable, ParamSpec, TypeVar, final

from melony.core.consumers import BaseConsumer
from melony.core.publishers import IPublisher
from melony.core.tasks import TaskWrapper

_TaskParams = ParamSpec("_TaskParams")
_TaskResult = TypeVar("_TaskResult")


class BaseBroker(ABC):
    @property
    @abstractmethod
    def publisher(self) -> IPublisher:
        ...

    @property
    @abstractmethod
    def consumer(self) -> BaseConsumer:
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
