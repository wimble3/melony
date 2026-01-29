import asyncio

from abc import ABC, abstractmethod
from functools import wraps
from inspect import signature
from typing import Callable, ParamSpec, TypeVar, final, overload

from melony.core.consumers import BaseConsumer
from melony.core.publishers import Publisher
from melony.core.task_wrappers import AsyncTaskWrapper, SyncTaskWrapper, TaskWrapper

__all__ = ()

_TaskParams = ParamSpec("_TaskParams")
_TaskResult = TypeVar("_TaskResult")


class BaseBroker(ABC):
    @property
    @abstractmethod
    def publisher(self) -> Publisher:
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
        """Decorator for registering your melony tasks.
        
        After your broker initizization (see selectetd broker documentation), 
        you are able to register task for next delaying or execution.
        
        Task declaration example:

            >>> import asyncio
 
            >>> @broker.task
            >>> async def example_task(string_param: str) -> str:
            ...     asyncio.sleep(2)
            ...     return string_param.upper()

        Now you have opportunity to call special method for execute your task after some 
        time. This method called .delay()

        Task delay example:
            >>> await example.task(string_param='my string').delay(countdown=30)  # will be executed after 30 seconds

        Or just execute this task as a common funcion immidiatly

        Task execution example:

            >>> example_task(string_param='execute now').execute()
        """
        self._validate_params(retries, retry_timeout)
        def _decorate(  # noqa: WPS430
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

                if asyncio.iscoroutinefunction(func):
                    return AsyncTaskWrapper(
                        func=func,
                        broker=self,
                        bound_args=bound.arguments,
                        retries=retries,
                        retry_timeout=retry_timeout
                    )
                else:
                    return SyncTaskWrapper(
                        func=func,
                        broker=self,
                        bound_args=bound.arguments,
                        retries=retries,
                        retry_timeout=retry_timeout
                    )

            wrapper.__annotations__ = {
                **func.__annotations__,
                "return": AsyncTaskWrapper
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
