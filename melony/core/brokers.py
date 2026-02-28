import asyncio

from abc import ABC, abstractmethod
from functools import wraps
from inspect import signature
from typing import TYPE_CHECKING, Callable, ParamSpec, TypeVar, final, overload

from croniter import croniter

from melony.core.consts import CRON_QUEUE_PREFIX, DEFAULT_QUEUE
from melony.core.consumers import BaseConsumer
from melony.core.cron_tasks import CronTaskRegistration
from melony.core.publishers import Publisher
from melony.core.task_wrappers import AsyncTaskWrapper, SyncTaskWrapper, TaskWrapper

if TYPE_CHECKING:
    from melony.core.cron_consumers import BaseCronConsumer

__all__ = ()

_TaskParams = ParamSpec("_TaskParams")
_TaskResult = TypeVar("_TaskResult")


class BaseBroker(ABC):
    def __init__(self) -> None:
        self._cron_registry: list[CronTaskRegistration] = []

    @property
    @abstractmethod
    def publisher(self) -> Publisher:
        ...

    @property
    @abstractmethod
    def consumer(self) -> BaseConsumer:
        ...

    @property
    @abstractmethod
    def cron_consumer(self) -> "BaseCronConsumer":
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
        retry_timeout: int = 0,
        queue: str = DEFAULT_QUEUE,
        cron: str | None = None
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
        queue: str = DEFAULT_QUEUE,
        cron: str | None = None
    ) -> Callable[_TaskParams, TaskWrapper] | Callable[[Callable], Callable]:
        """Decorator for registering your melony tasks.

        After your broker initizization (see selected broker documentation),
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

            >>> await example_task(string_param='execute now').execute()
        """
        self._validate_params(retries, retry_timeout, cron)
        def _decorate(  # noqa: WPS430
            func: Callable[_TaskParams, _TaskResult]
        ) -> Callable[_TaskParams, TaskWrapper]:
            if cron:
                self._cron_registry.append(CronTaskRegistration(
                    func_path=f"{func.__module__}.{func.__qualname__}",
                    func=func,
                    cron=cron,
                    queue=f"{CRON_QUEUE_PREFIX}{queue}",
                    retries=retries,
                    retry_timeout=retry_timeout,
                    is_coro=asyncio.iscoroutinefunction(func),
                ))

            @wraps(func)
            def wrapper(
                *args: _TaskParams.args,
                **kwargs: _TaskParams.kwargs
            ) -> TaskWrapper:
                sig = signature(func)
                bound = sig.bind(*args, **kwargs)
                bound.apply_defaults()

                if asyncio.iscoroutinefunction(func):
                    wrapper.__annotations__ = {
                        **func.__annotations__,
                        "return": AsyncTaskWrapper
                    }
                    return AsyncTaskWrapper(
                        func=func,
                        broker=self,
                        bound_args=bound.arguments,
                        retries=retries,
                        retry_timeout=retry_timeout,
                        queue=queue
                    )
                else:
                    wrapper.__annotations__ = {
                        **func.__annotations__,
                        "return": SyncTaskWrapper
                    }
                    return SyncTaskWrapper(
                        func=func,
                        broker=self,
                        bound_args=bound.arguments,
                        retries=retries,
                        retry_timeout=retry_timeout,
                        queue=queue
                    )


            return wrapper

        if func is None:
            return _decorate
        else:
            return _decorate(func)

    @final
    def _validate_params(
        self,
        retries: int,
        retry_timeout: int,
        cron: str | None = None,
    ) -> None:
        if retries <= 0:
            raise ValueError(
                "Parameter 'retries' must be positive integer (without zero)"
            )
        if retry_timeout < 0:
            raise ValueError(
                "Parameter 'retry_timeout' must be positive integer or zero"
            )
        if cron is not None and not croniter.is_valid(cron):
            raise ValueError(f"Invalid cron expression: '{cron}'")
