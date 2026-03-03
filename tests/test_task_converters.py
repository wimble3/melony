import json

from unittest.mock import MagicMock, patch

from melony.core.task_converters import TaskConverter
from melony.core.tasks import AsyncTask, SyncTask


async def _async_func() -> str:
    return "async_result"


def _sync_func() -> str:
    return "sync_result"


def _make_sync_task() -> SyncTask:
    return SyncTask(
        task_id="t-1",
        func=_sync_func,
        func_path="tests.test_task_converters._sync_func",
        kwargs={},
        broker=MagicMock(),
        countdown=0,
        retries=2,
        retry_timeout=0,
        queue="melony_tasks:default",
        is_coro=False,
    )


def _make_async_task() -> AsyncTask:
    return AsyncTask(
        task_id="t-2",
        func=_async_func,
        func_path="tests.test_task_converters._async_func",
        kwargs={},
        broker=MagicMock(),
        countdown=0,
        retries=2,
        retry_timeout=0,
        queue="melony_tasks:default",
        is_coro=True,
    )


def test_serialize_task_returns_valid_json():
    converter = TaskConverter()
    task = _make_sync_task()
    serialized = converter.serialize_task(task)
    data = json.loads(serialized)
    assert data["task_id"] == "t-1"
    assert data["is_coro"] is False


def test_deserialize_sync_task_returns_sync_task():
    converter = TaskConverter()
    task = _make_sync_task()
    serialized = converter.serialize_task(task)

    with patch("melony.core.task_converters.find_task_func", return_value=_sync_func):
        result = converter.deserialize_task(serialized, MagicMock())

    assert isinstance(result, SyncTask)
    assert result.task_id == "t-1"
    assert result.retries == 2


def test_deserialize_async_task_returns_async_task():
    converter = TaskConverter()
    task = _make_async_task()
    serialized = converter.serialize_task(task)

    with patch("melony.core.task_converters.find_task_func", return_value=_async_func):
        result = converter.deserialize_task(serialized, MagicMock())

    assert isinstance(result, AsyncTask)
    assert result.task_id == "t-2"
    assert result.is_coro is True


def test_serialize_deserialize_round_trip_preserves_kwargs():
    task = SyncTask(
        task_id="t-3",
        func=_sync_func,
        func_path="tests.test_task_converters._sync_func",
        kwargs={"x": 1, "y": 2},
        broker=MagicMock(),
        countdown=5,
        retries=3,
        retry_timeout=10,
        queue="melony_tasks:custom",
        is_coro=False,
    )
    converter = TaskConverter()
    serialized = converter.serialize_task(task)

    with patch("melony.core.task_converters.find_task_func", return_value=_sync_func):
        result = converter.deserialize_task(serialized, MagicMock())

    assert result.kwargs == {"x": 1, "y": 2}
    assert result.countdown == 5
    assert result.retry_timeout == 10
    assert result.queue == "melony_tasks:custom"
