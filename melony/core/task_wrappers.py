from collections.abc import Awaitable, Callable
from inspect import signature
from typing import TYPE_CHECKING, Any, ParamSpec, TypeVar, final
from uuid import uuid4

from melony.core.consts import QUEUE_PREFIX
from melony.core.tasks import AsyncTask, SyncTask, Task
from melony.logger import log_info

if TYPE_CHECKING:
    from melony.core.brokers import BaseBroker

__all__ = ()


type TaskWrapper = AsyncTaskWrapper | SyncTaskWrapper

_TaskParams = ParamSpec("_TaskParams")
_TaskResult = TypeVar("_TaskResult")


class _BaseTaskWrapper(Awaitable):
    @final
    def __init__(
        self,
        func: Callable[_TaskParams, _TaskResult],
        broker: "BaseBroker",
        bound_args: dict[str, Any],
        retries: int,
        retry_timeout: int,
        queue: str
    ) -> None:
        self._func = func
        self._broker = broker
        self._sig = signature(func)
        self._bound_args = bound_args or {}
        self._func_path = f"{func.__module__}.{func.__qualname__}"
        self._retries = retries
        self._retry_timeout = retry_timeout
        self._queue = f"{QUEUE_PREFIX}{queue}"

    @final
    def __call__(
        self,
        *args: _TaskParams.args,  # type: ignore
        **kwargs: _TaskParams.kwargs
    ) -> "_BaseTaskWrapper":
        bound = self._sig.bind(*args, **kwargs)
        bound.apply_defaults()
        self._bound_args = bound.arguments
        return self

    @final
    def __await__(self) -> Any:
        raise RuntimeError(
            "Task cannot be awaited, did you forget to call .delay()?"
        )

@final
class AsyncTaskWrapper(_BaseTaskWrapper):
    async def delay(self, countdown: int = 0) -> AsyncTask:
        from melony.core.publishers import IAsyncPublisher
        assert isinstance(self._broker.publisher, IAsyncPublisher)
        task = AsyncTask(
            task_id=str(uuid4()),
            func=self._func,
            func_path=self._func_path,
            kwargs=self._bound_args,
            broker=self._broker,
            countdown=countdown,
            retries=self._retries,
            retry_timeout=self._retry_timeout,
            queue=self._queue,
            is_coro=True
        )
        await self._broker.publisher.push(task)
        log_info(f"Pushed task {task} to queue {task.queue}")
        return task


@final
class SyncTaskWrapper(_BaseTaskWrapper):
    def delay(self, countdown: int = 0) -> Awaitable[SyncTask]:  # TODO: need to fix typing here
        from melony.core.publishers import ISyncPublisher
        assert isinstance(self._broker.publisher, ISyncPublisher)
        task = SyncTask(
            task_id=str(uuid4()),
            func=self._func,
            func_path=self._func_path,
            kwargs=self._bound_args,
            broker=self._broker,
            countdown=countdown,
            retries=self._retries,
            retry_timeout=self._retry_timeout,
            queue=self._queue,
            is_coro=False
        )
        self._broker.publisher.push(task)
        log_info(f"Pushed task {task} to queue {task.queue}")
        return task  # type: ignore
