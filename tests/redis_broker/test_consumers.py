from unittest.mock import AsyncMock, MagicMock, patch

from melony.redis_broker.consumers import AsyncRedisConsumer, SyncRedisConsumer
from melony.core.tasks import SyncTask, AsyncTask


async def _async_func() -> str:
    return "result"


def _sync_func() -> str:
    return "result"


def _make_sync_task(task_id: str = "t-1") -> SyncTask:
    return SyncTask(
        task_id=task_id,
        func=_sync_func,
        func_path="tests.redis_broker.test_consumers._sync_func",
        kwargs={},
        broker=MagicMock(),
        countdown=0,
        retries=1,
        retry_timeout=0,
        queue="melony_tasks:default",
        is_coro=False,
    )


def _make_async_task(task_id: str = "t-2") -> AsyncTask:
    return AsyncTask(
        task_id=task_id,
        func=_async_func,
        func_path="tests.redis_broker.test_consumers._async_func",
        kwargs={},
        broker=MagicMock(),
        countdown=0,
        retries=1,
        retry_timeout=0,
        queue="melony_tasks:default",
        is_coro=True,
    )


def _async_publisher(conn):
    publisher = MagicMock()
    publisher.connection = conn
    return publisher


def _sync_publisher(conn):
    publisher = MagicMock()
    publisher.connection = conn
    return publisher


def test_async_redis_consumer_init():
    conn = AsyncMock()
    publisher = _async_publisher(conn)
    consumer = AsyncRedisConsumer(publisher=publisher, broker=MagicMock())
    assert consumer._broker is not None
    assert consumer._task_converter is not None


async def test_async_redis_consumer_pop_tasks_returns_one_task():
    conn = AsyncMock()
    task = _make_sync_task()
    serialized = task.as_json().encode("utf-8")

    conn.brpop = AsyncMock(side_effect=[
        (b"melony_tasks:default", serialized),
        None,
    ])
    publisher = _async_publisher(conn)
    with patch("melony.core.finders.find_task_func", return_value=_sync_func):
        consumer = AsyncRedisConsumer(publisher=publisher, broker=MagicMock())
        tasks = await consumer._pop_tasks("melony_tasks:default")

    assert len(tasks) == 1
    assert tasks[0].task_id == "t-1"


async def test_async_redis_consumer_pop_tasks_returns_multiple_tasks():
    conn = AsyncMock()
    t1 = _make_sync_task("task-a")
    t2 = _make_sync_task("task-b")

    conn.brpop = AsyncMock(side_effect=[
        (b"q", t1.as_json().encode()),
        (b"q", t2.as_json().encode()),
        None,
    ])
    publisher = _async_publisher(conn)
    with patch("melony.core.finders.find_task_func", return_value=_sync_func):
        consumer = AsyncRedisConsumer(publisher=publisher, broker=MagicMock())
        tasks = await consumer._pop_tasks("melony_tasks:default")

    assert len(tasks) == 2


async def test_async_redis_consumer_deserialize_task():
    conn = AsyncMock()
    publisher = _async_publisher(conn)
    consumer = AsyncRedisConsumer(publisher=publisher, broker=MagicMock())
    task = _make_sync_task()
    serialized = task.as_json().encode("utf-8")

    with patch("melony.core.finders.find_task_func", return_value=_sync_func):
        result = consumer._deserialize_to_task_from_redis((b"queue", serialized))

    assert result.task_id == "t-1"
    assert isinstance(result, SyncTask)


def test_sync_redis_consumer_init():
    conn = MagicMock()
    publisher = _sync_publisher(conn)
    consumer = SyncRedisConsumer(publisher=publisher, broker=MagicMock())
    assert consumer._broker is not None
    assert consumer._task_converter is not None


def test_sync_redis_consumer_pop_tasks_returns_one_task():
    conn = MagicMock()
    task = _make_sync_task()
    serialized = task.as_json().encode("utf-8")

    conn.brpop.side_effect = [
        (b"melony_tasks:default", serialized),
        None,
    ]
    publisher = _sync_publisher(conn)
    with patch("melony.core.finders.find_task_func", return_value=_sync_func):
        consumer = SyncRedisConsumer(publisher=publisher, broker=MagicMock())
        tasks = consumer._pop_tasks("melony_tasks:default")

    assert len(tasks) == 1
    assert tasks[0].task_id == "t-1"


def test_sync_redis_consumer_pop_tasks_returns_multiple_tasks():
    conn = MagicMock()
    t1 = _make_sync_task("ta")
    t2 = _make_sync_task("tb")

    conn.brpop.side_effect = [
        (b"q", t1.as_json().encode()),
        (b"q", t2.as_json().encode()),
        None,
    ]
    publisher = _sync_publisher(conn)
    with patch("melony.core.finders.find_task_func", return_value=_sync_func):
        consumer = SyncRedisConsumer(publisher=publisher, broker=MagicMock())
        tasks = consumer._pop_tasks("melony_tasks:default")

    assert len(tasks) == 2


def test_sync_redis_consumer_deserialize_task():
    conn = MagicMock()
    publisher = _sync_publisher(conn)
    consumer = SyncRedisConsumer(publisher=publisher, broker=MagicMock())
    task = _make_sync_task()
    serialized = task.as_json().encode("utf-8")

    with patch("melony.core.finders.find_task_func", return_value=_sync_func):
        result = consumer._deserialize_to_task_from_redis((b"queue", serialized))

    assert result.task_id == "t-1"
    assert isinstance(result, SyncTask)
