from unittest.mock import AsyncMock, MagicMock

import pytest

from melony.redis_broker.result_backends import (
    RedisResultBackend,
    _AsyncRedisResultBackendSaver,
    _SyncRedisResultBackendSaver,
)
from melony.core.result_backends import IAsyncResultBackendSaver, ISyncResultBackendSaver
from redis.asyncio import Redis as AsyncRedis
from redis import Redis as SyncRedis


class _UnknownConn:
    pass


def _make_task_result(task_id: str = "t-1", result="done"):
    task = MagicMock()
    task.task_id = task_id
    task.as_dict.return_value = {"task_id": task_id}
    dto = MagicMock()
    dto.task = task
    dto.task_result = result
    return dto


def test_result_backend_init_stores_connection():
    conn = MagicMock(spec=AsyncRedis)
    backend = RedisResultBackend(redis_connection=conn)
    assert backend._connection is conn


def test_result_backend_async_connection_returns_async_saver():
    conn = MagicMock(spec=AsyncRedis)
    backend = RedisResultBackend(redis_connection=conn)
    assert isinstance(backend.saver, _AsyncRedisResultBackendSaver)


def test_result_backend_sync_connection_returns_sync_saver():
    conn = MagicMock(spec=SyncRedis)
    backend = RedisResultBackend(redis_connection=conn)
    assert isinstance(backend.saver, _SyncRedisResultBackendSaver)


def test_async_saver_is_instance_of_interface():
    conn = MagicMock(spec=AsyncRedis)
    backend = RedisResultBackend(redis_connection=conn)
    assert isinstance(backend.saver, IAsyncResultBackendSaver)


def test_sync_saver_is_instance_of_interface():
    conn = MagicMock(spec=SyncRedis)
    backend = RedisResultBackend(redis_connection=conn)
    assert isinstance(backend.saver, ISyncResultBackendSaver)


async def test_async_saver_calls_redis_set_for_each_result():
    conn = AsyncMock()
    saver = _AsyncRedisResultBackendSaver(redis_connection=conn)
    results = [_make_task_result("t-1"), _make_task_result("t-2")]
    await saver.save_results(results)
    assert conn.set.call_count == 2


async def test_async_saver_uses_correct_redis_key():
    conn = AsyncMock()
    saver = _AsyncRedisResultBackendSaver(redis_connection=conn)
    dto = _make_task_result("abc-123")
    await saver.save_results([dto])
    call_kwargs = conn.set.call_args.kwargs
    assert "melony_result_backend:abc-123" in call_kwargs["name"]


async def test_async_saver_empty_results_makes_no_calls():
    conn = AsyncMock()
    saver = _AsyncRedisResultBackendSaver(redis_connection=conn)
    await saver.save_results([])
    conn.set.assert_not_called()


def test_sync_saver_calls_redis_set_for_each_result():
    conn = MagicMock()
    saver = _SyncRedisResultBackendSaver(redis_connection=conn)
    results = [_make_task_result("t-1"), _make_task_result("t-2")]
    saver.save_results(results)
    assert conn.set.call_count == 2


def test_sync_saver_uses_correct_redis_key():
    conn = MagicMock()
    saver = _SyncRedisResultBackendSaver(redis_connection=conn)
    dto = _make_task_result("xyz-456")
    saver.save_results([dto])
    call_kwargs = conn.set.call_args.kwargs
    assert "melony_result_backend:xyz-456" in call_kwargs["name"]


def test_sync_saver_empty_results_makes_no_calls():
    conn = MagicMock()
    saver = _SyncRedisResultBackendSaver(redis_connection=conn)
    saver.save_results([])
    conn.set.assert_not_called()


def test_result_backend_unknown_connection_type_raises():
    backend = RedisResultBackend(redis_connection=_UnknownConn())  # type: ignore
    with pytest.raises(Exception):
        _ = backend.saver
