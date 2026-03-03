from unittest.mock import AsyncMock, MagicMock, patch

from melony.redis_broker.publishers import AsyncRedisPublisher, SyncRedisPublisher
from melony.core.tasks import AsyncTask, SyncTask


async def _target() -> str:
    return "result"


def _sync_target() -> str:
    return "result"


def _make_async_task() -> AsyncTask:
    return AsyncTask(
        task_id="t-1",
        func=_target,
        func_path="tests.redis_broker.test_publishers._target",
        kwargs={},
        broker=MagicMock(),
        countdown=0,
        retries=1,
        retry_timeout=0,
        queue="melony_tasks:default",
        is_coro=True,
    )


def _make_sync_task() -> SyncTask:
    return SyncTask(
        task_id="t-2",
        func=_sync_target,
        func_path="tests.redis_broker.test_publishers._sync_target",
        kwargs={},
        broker=MagicMock(),
        countdown=0,
        retries=1,
        retry_timeout=0,
        queue="melony_tasks:default",
        is_coro=False,
    )


def test_async_publisher_connection_property():
    mock_conn = AsyncMock()
    publisher = AsyncRedisPublisher(connection=mock_conn)
    assert publisher.connection is mock_conn


async def test_async_publisher_push_calls_lpush():
    mock_conn = AsyncMock()
    publisher = AsyncRedisPublisher(connection=mock_conn)
    task = _make_async_task()
    await publisher.push(task)
    mock_conn.lpush.assert_called_once()
    call_args = mock_conn.lpush.call_args
    assert call_args[0][0] == task.queue


async def test_async_publisher_push_serializes_task():
    mock_conn = AsyncMock()
    publisher = AsyncRedisPublisher(connection=mock_conn)
    task = _make_async_task()
    with patch.object(publisher._task_converter, "serialize_task", return_value="serialized") as mock_ser:
        await publisher.push(task)
        mock_ser.assert_called_once_with(task)
    mock_conn.lpush.assert_called_once_with(task.queue, "serialized")


def test_sync_publisher_connection_property():
    mock_conn = MagicMock()
    publisher = SyncRedisPublisher(connection=mock_conn)
    assert publisher.connection is mock_conn


def test_sync_publisher_push_calls_lpush():
    mock_conn = MagicMock()
    publisher = SyncRedisPublisher(connection=mock_conn)
    task = _make_sync_task()
    publisher.push(task)
    mock_conn.lpush.assert_called_once()
    call_args = mock_conn.lpush.call_args
    assert call_args[0][0] == task.queue


def test_sync_publisher_push_serializes_task():
    mock_conn = MagicMock()
    publisher = SyncRedisPublisher(connection=mock_conn)
    task = _make_sync_task()
    with patch.object(publisher._task_converter, "serialize_task", return_value="serialized") as mock_ser:
        publisher.push(task)
        mock_ser.assert_called_once_with(task)
    mock_conn.lpush.assert_called_once_with(task.queue, "serialized")
