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
    def task(self, retries: int = 1, retry_timeout: int = 0):
        def decorator(func: Callable[_TaskParams, _TaskResult]):
            @wraps(func)
            def wrapper(*args, **kwargs):
                sig = signature(func)
                bound = sig.bind(*args, **kwargs)
                bound.apply_defaults()

                return TaskWrapper(
                    func=func,
                    broker=self,
                    bound_args=bound.arguments,
                    retries=retries,
                    retry_timeout=retry_timeout
                )

            wrapper.__annotations__ = {
                **func.__annotations__,
                "return": TaskWrapper
            }
            return wrapper
        return decorator
