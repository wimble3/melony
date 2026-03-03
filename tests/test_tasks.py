import json

import pytest

from unittest.mock import MagicMock

from melony.core.tasks import AsyncTask, SyncTask, _TaskMeta


async def _async_noop() -> str:
    return "done"


async def _async_add(a: int, b: int) -> int:
    return a + b


def _sync_noop() -> str:
    return "done"


def _sync_add(a: int, b: int) -> int:
    return a + b


def _make_sync_task(**kwargs) -> SyncTask:
    defaults = dict(
        task_id="t-sync",
        func=_sync_noop,
        func_path="tests.test_tasks._sync_noop",
        kwargs={},
        broker=MagicMock(),
        countdown=0,
        retries=3,
        retry_timeout=0,
        queue="melony_tasks:default",
        is_coro=False,
    )
    return SyncTask(**{**defaults, **kwargs})


def _make_async_task(**kwargs) -> AsyncTask:
    defaults = dict(
        task_id="t-async",
        func=_async_noop,
        func_path="tests.test_tasks._async_noop",
        kwargs={},
        broker=MagicMock(),
        countdown=0,
        retries=3,
        retry_timeout=0,
        queue="melony_tasks:default",
        is_coro=True,
    )
    return AsyncTask(**{**defaults, **kwargs})


def test_task_meta_default_retries_left_is_none():
    meta = _TaskMeta()
    assert meta.retries_left is None


def test_task_meta_accepts_explicit_retries_left():
    meta = _TaskMeta(retries_left=5)
    assert meta.retries_left == 5


def test_negative_countdown_raises_value_error():
    with pytest.raises(ValueError, match="negative"):
        _make_sync_task(countdown=-1)


def test_countdown_at_max_raises_value_error():
    with pytest.raises(ValueError, match="86400"):
        _make_sync_task(countdown=86400)


def test_countdown_zero_is_valid():
    task = _make_sync_task(countdown=0)
    assert task.countdown == 0


def test_countdown_just_below_max_is_valid():
    task = _make_sync_task(countdown=86399)
    assert task.countdown == 86399


def test_retries_left_set_from_retries_when_meta_not_provided():
    task = _make_sync_task(retries=5)
    assert task._meta.retries_left == 5


def test_retries_left_preserved_when_already_set_in_meta():
    meta = _TaskMeta(retries_left=2)
    task = _make_sync_task(retries=5, _meta=meta)
    assert task._meta.retries_left == 2


def test_timestamp_returns_meta_timestamp():
    task = _make_sync_task()
    assert task.timestamp == task._meta.timestamp


def test_get_execution_timestamp_no_countdown():
    task = _make_sync_task(countdown=0)
    assert task.get_execution_timestamp() == task.timestamp


def test_get_execution_timestamp_with_countdown():
    task = _make_sync_task(countdown=10)
    assert task.get_execution_timestamp() == task.timestamp + 10


def test_as_json_serializable_obj_has_correct_task_id():
    task = _make_sync_task()
    obj = task.as_json_serializable_obj()
    assert obj.task_id == task.task_id


def test_as_json_serializable_obj_has_func_name():
    task = _make_sync_task()
    obj = task.as_json_serializable_obj()
    assert obj.func_name == "_sync_noop"


def test_as_dict_returns_dict_with_task_id():
    task = _make_sync_task()
    d = task.as_dict()
    assert isinstance(d, dict)
    assert d["task_id"] == "t-sync"


def test_as_json_produces_valid_json():
    task = _make_sync_task()
    data = json.loads(task.as_json())
    assert data["task_id"] == "t-sync"
    assert data["is_coro"] is False


def test_as_json_async_task_is_coro_true():
    task = _make_async_task()
    data = json.loads(task.as_json())
    assert data["is_coro"] is True


async def test_async_task_execute_returns_result():
    task = _make_async_task()
    result = await task.execute()
    assert result == "done"


async def test_async_task_execute_passes_kwargs():
    task = _make_async_task(func=_async_add, kwargs={"a": 3, "b": 4}, is_coro=True)
    result = await task.execute()
    assert result == 7


def test_sync_task_execute_returns_result():
    task = _make_sync_task()
    result = task.execute()
    assert result == "done"


def test_sync_task_execute_passes_kwargs():
    task = _make_sync_task(func=_sync_add, kwargs={"a": 10, "b": 5})
    result = task.execute()
    assert result == 15
