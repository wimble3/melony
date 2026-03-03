import asyncio

from unittest.mock import AsyncMock, MagicMock

import pytest

from melony.core.result_backends import IAsyncResultBackendSaver, ISyncResultBackendSaver
from melony.core.task_executor import AsyncTaskExecutor, SyncTaskExecutor
from melony.core.tasks import AsyncTask, SyncTask, _TaskMeta


async def _noop() -> str:
    return "ok"


async def _failing() -> None:
    raise ValueError("task failed")


def _sync_noop() -> str:
    return "sync_ok"


def _sync_failing() -> None:
    raise ValueError("sync failed")


def _make_async_task(func=_noop, retries_left=3) -> AsyncTask:
    meta = _TaskMeta(retries_left=retries_left)
    return AsyncTask(
        task_id="a-1",
        func=func,
        func_path="tests.test_task_executor._noop",
        kwargs={},
        broker=MagicMock(),
        countdown=0,
        retries=retries_left,
        retry_timeout=0,
        queue="melony_tasks:default",
        is_coro=True,
        _meta=meta,
    )


def _make_sync_task(func=_sync_noop, retries_left=3) -> SyncTask:
    meta = _TaskMeta(retries_left=retries_left)
    return SyncTask(
        task_id="s-1",
        func=func,
        func_path="tests.test_task_executor._sync_noop",
        kwargs={},
        broker=MagicMock(),
        countdown=0,
        retries=retries_left,
        retry_timeout=0,
        queue="melony_tasks:default",
        is_coro=False,
        _meta=meta,
    )


async def test_async_executor_init_without_result_backend():
    executor = AsyncTaskExecutor(connection=MagicMock())
    assert executor._result_backend is None


async def test_async_executor_init_with_result_backend():
    backend = MagicMock(spec=IAsyncResultBackendSaver)
    executor = AsyncTaskExecutor(connection=MagicMock(), result_backend_saver=backend)
    assert executor._result_backend is backend


async def test_async_executor_successful_task_in_done_list():
    executor = AsyncTaskExecutor(connection=MagicMock())
    task = _make_async_task()
    result = await executor.execute_tasks([task], consumer_id=0)
    assert len(list(result.tasks_with_result)) == 1
    assert len(list(result.tasks_to_retry)) == 0


async def test_async_executor_failed_task_in_retry_list():
    executor = AsyncTaskExecutor(connection=MagicMock())
    task = _make_async_task(func=_failing)
    result = await executor.execute_tasks([task], consumer_id=0)
    assert len(list(result.tasks_to_retry)) == 1
    assert len(list(result.tasks_with_result)) == 0


async def test_async_executor_decrements_retries_left():
    task = _make_async_task(retries_left=3)
    executor = AsyncTaskExecutor(connection=MagicMock())
    await executor.execute_tasks([task], consumer_id=0)
    assert task._meta.retries_left == 2


async def test_async_executor_calls_result_backend_on_success():
    mock_backend = MagicMock(spec=IAsyncResultBackendSaver)
    mock_backend.save_results = AsyncMock()
    executor = AsyncTaskExecutor(
        connection=MagicMock(), result_backend_saver=mock_backend
    )
    task = _make_async_task()
    await executor.execute_tasks([task], consumer_id=0)
    mock_backend.save_results.assert_called_once()


async def test_async_executor_empty_task_list_returns_empty_results():
    executor = AsyncTaskExecutor(connection=MagicMock())
    result = await executor.execute_tasks([], consumer_id=0)
    assert list(result.tasks_with_result) == []
    assert list(result.tasks_to_retry) == []


async def test_get_task_result_returns_value_on_success():
    executor = AsyncTaskExecutor(connection=MagicMock())

    async def coro():
        return 42

    asyncio_task = asyncio.create_task(coro())
    await asyncio_task
    result = executor._get_task_result(asyncio_task=asyncio_task)
    assert result == 42


async def test_get_task_result_returns_exception_on_failure():
    executor = AsyncTaskExecutor(connection=MagicMock())

    async def failing_coro():
        raise ValueError("boom")

    asyncio_task = asyncio.create_task(failing_coro())
    with pytest.raises(ValueError):
        await asyncio_task
    result = executor._get_task_result(asyncio_task=asyncio_task)
    assert isinstance(result, ValueError)


def test_sync_executor_init_without_result_backend():
    executor = SyncTaskExecutor(connection=MagicMock())
    assert executor._result_backend_saver is None


def test_sync_executor_init_with_result_backend():
    backend = MagicMock(spec=ISyncResultBackendSaver)
    executor = SyncTaskExecutor(connection=MagicMock(), result_backend_saver=backend)
    assert executor._result_backend_saver is backend


def test_sync_executor_successful_task_in_done_list():
    executor = SyncTaskExecutor(connection=MagicMock())
    task = _make_sync_task()
    result = executor.execute_tasks([task], consumer_id=0)
    assert len(list(result.tasks_with_result)) == 1
    assert len(list(result.tasks_to_retry)) == 0


def test_sync_executor_failed_task_in_retry_list():
    executor = SyncTaskExecutor(connection=MagicMock())
    task = _make_sync_task(func=_sync_failing)
    result = executor.execute_tasks([task], consumer_id=0)
    assert len(list(result.tasks_to_retry)) == 1
    assert len(list(result.tasks_with_result)) == 0


def test_sync_executor_decrements_retries_left():
    task = _make_sync_task(retries_left=3)
    executor = SyncTaskExecutor(connection=MagicMock())
    executor.execute_tasks([task], consumer_id=0)
    assert task._meta.retries_left == 2


def test_sync_executor_calls_result_backend_on_success():
    mock_backend = MagicMock(spec=ISyncResultBackendSaver)
    executor = SyncTaskExecutor(
        connection=MagicMock(), result_backend_saver=mock_backend
    )
    task = _make_sync_task()
    executor.execute_tasks([task], consumer_id=0)
    mock_backend.save_results.assert_called_once()


def test_sync_executor_empty_task_list_returns_empty_results():
    executor = SyncTaskExecutor(connection=MagicMock())
    result = executor.execute_tasks([], consumer_id=0)
    assert list(result.tasks_with_result) == []
    assert list(result.tasks_to_retry) == []
