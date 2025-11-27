from abc import ABC, abstractmethod
from functools import wraps
from inspect import signature
from typing import Callable, ParamSpec, TypeVar, final, overload

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

    @overload
    def task(
        self,
        func: Callable[_TaskParams, _TaskResult]
    ) -> Callable[_TaskParams, TaskWrapper]:
        ...
    
    @overload
    def task(
        self,
        *, 
        retries: int = 1,
        retry_timeout: int = 0
    ) -> Callable[
            [Callable[_TaskParams, _TaskResult]],  # type: ignore
            Callable[_TaskParams, TaskWrapper]
        ]:
        ...

    @final
    def task(
        self,
        func: Callable[_TaskParams, _TaskResult] | None = None,
        *,
        retries: int = 1,
        retry_timeout: int = 0,
    ) -> Callable[_TaskParams, TaskWrapper] | Callable[[Callable], Callable]:
        self._validate_params(retries, retry_timeout)
        def _decorate(
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
                    bound_args=bound.arguments,
                    retries=retries,
                    retry_timeout=retry_timeout
                )

            wrapper.__annotations__ = {
                **func.__annotations__,
                "return": TaskWrapper
            }
            return wrapper

        if func is None:
            return _decorate
        else:
            return _decorate(func)

    @final
    def _validate_params(self, retries: int, retry_timeout: int) -> None:
        if retries <= 0:
            raise ValueError(
                "Parameter 'retries' must be positive integer (without zero)"
            )
        if retry_timeout < 0:
            raise ValueError(
                "Parameter 'retry_timeout' must be positive integer or zero"
            )

