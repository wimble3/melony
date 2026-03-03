import pytest

from unittest.mock import MagicMock

from melony.core.publishers import IAsyncPublisher, ISyncPublisher
from melony.core.tasks import AsyncTask, SyncTask
from melony.core.task_wrappers import AsyncTaskWrapper, SyncTaskWrapper
from redis.asyncio import Redis as AsyncRedis
from redis import Redis as SyncRedis


class _StubAsyncPublisher(IAsyncPublisher):
    def __init__(self):
        self._pushed = []

    @property
    def connection(self) -> AsyncRedis:
        return MagicMock()

    async def push(self, task) -> None:
        self._pushed.append(task)


class _StubSyncPublisher(ISyncPublisher):
    def __init__(self):
        self._pushed = []

    @property
    def connection(self) -> SyncRedis:
        return MagicMock()

    def push(self, task) -> None:
        self._pushed.append(task)


def _async_broker(publisher=None):
    broker = MagicMock()
    broker.publisher = publisher or _StubAsyncPublisher()
    return broker


def _sync_broker(publisher=None):
    broker = MagicMock()
    broker.publisher = publisher or _StubSyncPublisher()
    return broker


async def _target_func(x: int = 0) -> int:
    return x


def _sync_target(x: int = 0) -> int:
    return x


def test_wrapper_call_updates_bound_args():
    wrapper = SyncTaskWrapper(
        func=_sync_target,
        broker=_sync_broker(),
        bound_args={"x": 1},
        retries=1,
        retry_timeout=0,
        queue="default",
    )
    result = wrapper(x=99)
    assert result._bound_args["x"] == 99


def test_async_wrapper_call_updates_bound_args():
    wrapper = AsyncTaskWrapper(
        func=_target_func,
        broker=_async_broker(),
        bound_args={"x": 1},
        retries=1,
        retry_timeout=0,
        queue="default",
    )
    result = wrapper(x=42)
    assert result._bound_args["x"] == 42


async def test_wrapper_await_raises_runtime_error():
    wrapper = SyncTaskWrapper(
        func=_sync_target,
        broker=_sync_broker(),
        bound_args={},
        retries=1,
        retry_timeout=0,
        queue="default",
    )
    with pytest.raises(RuntimeError, match="delay"):
        await wrapper


async def test_async_wrapper_delay_returns_async_task():
    publisher = _StubAsyncPublisher()
    wrapper = AsyncTaskWrapper(
        func=_target_func,
        broker=_async_broker(publisher),
        bound_args={"x": 7},
        retries=2,
        retry_timeout=0,
        queue="default",
    )
    task = await wrapper.delay()
    assert isinstance(task, AsyncTask)
    assert task.kwargs == {"x": 7}


async def test_async_wrapper_delay_pushes_task_to_publisher():
    publisher = _StubAsyncPublisher()
    wrapper = AsyncTaskWrapper(
        func=_target_func,
        broker=_async_broker(publisher),
        bound_args={},
        retries=1,
        retry_timeout=0,
        queue="default",
    )
    await wrapper.delay(countdown=5)
    assert len(publisher._pushed) == 1
    assert publisher._pushed[0].countdown == 5


async def test_async_wrapper_delay_task_has_correct_retries():
    publisher = _StubAsyncPublisher()
    wrapper = AsyncTaskWrapper(
        func=_target_func,
        broker=_async_broker(publisher),
        bound_args={},
        retries=4,
        retry_timeout=10,
        queue="default",
    )
    task = await wrapper.delay()
    assert task.retries == 4
    assert task.retry_timeout == 10


def test_sync_wrapper_delay_returns_sync_task():
    publisher = _StubSyncPublisher()
    wrapper = SyncTaskWrapper(
        func=_sync_target,
        broker=_sync_broker(publisher),
        bound_args={"x": 3},
        retries=1,
        retry_timeout=0,
        queue="default",
    )
    task = wrapper.delay()
    assert isinstance(task, SyncTask)
    assert task.kwargs == {"x": 3}


def test_sync_wrapper_delay_pushes_task_to_publisher():
    publisher = _StubSyncPublisher()
    wrapper = SyncTaskWrapper(
        func=_sync_target,
        broker=_sync_broker(publisher),
        bound_args={},
        retries=1,
        retry_timeout=0,
        queue="default",
    )
    wrapper.delay(countdown=0)
    assert len(publisher._pushed) == 1


def test_sync_wrapper_delay_task_has_correct_queue_prefix():
    publisher = _StubSyncPublisher()
    wrapper = SyncTaskWrapper(
        func=_sync_target,
        broker=_sync_broker(publisher),
        bound_args={},
        retries=1,
        retry_timeout=0,
        queue="reports",
    )
    wrapper.delay()
    pushed_task = publisher._pushed[0]
    assert pushed_task.queue == "melony_tasks:reports"
