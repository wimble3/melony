import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from redis.exceptions import ConnectionError

from melony.core.cron_consumers import BaseAsyncCronConsumer, BaseSyncCronConsumer
from melony.core.cron_tasks import CronEntry, CronTaskRegistration
from melony.redis_broker.cron_consumers import AsyncRedisCronConsumer, SyncRedisCronConsumer


def _make_entry(**kwargs) -> CronEntry:
    defaults = dict(
        func_path="mymod.my_func",
        cron="* * * * *",
        queue="melony_cron:default",
        retries=1,
        retry_timeout=0,
        is_coro=False,
    )
    return CronEntry(**{**defaults, **kwargs})


class _StubAsyncCronConsumer(BaseAsyncCronConsumer):
    _poll_interval_secs = 0

    def __init__(self, connection=None, broker=None, entries=None, raise_after=None):
        super().__init__(connection=connection or MagicMock(), broker=broker or MagicMock())
        self._entries = list(entries or [])
        self._raise_after = raise_after
        self._initialized = []
        self._rescheduled = []
        self._call_count = 0

    async def _pop_due_entries(self, cron_queue: str) -> list[CronEntry]:
        self._call_count += 1
        if self._raise_after is not None and self._call_count > self._raise_after:
            raise ConnectionError("done")
        return list(self._entries)

    async def _initialize_cron_tasks(self, cron_queue: str) -> None:
        self._initialized.append(cron_queue)

    async def _reschedule(self, entry: CronEntry, cron_queue: str) -> None:
        self._rescheduled.append(entry)


class _StubSyncCronConsumer(BaseSyncCronConsumer):
    _poll_interval_secs = 0

    def __init__(self, connection=None, broker=None, entries=None, raise_after=None):
        super().__init__(connection=connection or MagicMock(), broker=broker or MagicMock())
        self._entries = list(entries or [])
        self._raise_after = raise_after
        self._initialized = []
        self._rescheduled = []
        self._call_count = 0

    def _pop_due_entries(self, cron_queue: str) -> list[CronEntry]:
        self._call_count += 1
        if self._raise_after is not None and self._call_count > self._raise_after:
            raise ConnectionError("done")
        return list(self._entries)

    def _initialize_cron_tasks(self, cron_queue: str) -> None:
        self._initialized.append(cron_queue)

    def _reschedule(self, entry: CronEntry, cron_queue: str) -> None:
        self._rescheduled.append(entry)


def test_base_cron_consumer_run_all_processes_starts_and_joins():
    consumer = _StubSyncCronConsumer()
    p1, p2 = MagicMock(), MagicMock()
    consumer._run_all_processes([p1, p2])
    p1.start.assert_called_once()
    p1.join.assert_called_once()
    p2.start.assert_called_once()
    p2.join.assert_called_once()


async def test_async_start_consume_rejects_zero_processes():
    consumer = _StubAsyncCronConsumer()
    with pytest.raises(ValueError, match="processes"):
        await consumer.start_consume(processes=0)


async def test_async_start_consume_single_process_runs_loop():
    consumer = _StubAsyncCronConsumer(raise_after=0)
    await consumer.start_consume(queue="default", processes=1)
    assert "melony_cron:default" in consumer._initialized


async def test_async_start_consume_multiple_processes():
    consumer = _StubAsyncCronConsumer(raise_after=0)
    with patch("multiprocessing.Process") as mock_proc_cls:
        mock_proc = MagicMock()
        mock_proc_cls.return_value = mock_proc
        with patch.object(consumer, "_run_all_processes") as mock_run:
            await consumer.start_consume(processes=2)
            assert mock_proc_cls.call_count == 2
            mock_run.assert_called_once()


async def test_async_start_consume_terminates_on_keyboard_interrupt():
    consumer = _StubAsyncCronConsumer(raise_after=0)
    with patch("multiprocessing.Process") as mock_proc_cls:
        mock_proc = MagicMock()
        mock_proc.is_alive.return_value = True
        mock_proc_cls.return_value = mock_proc
        with patch.object(consumer, "_run_all_processes", side_effect=KeyboardInterrupt):
            await consumer.start_consume(processes=2)
        mock_proc.terminate.assert_called()


async def test_async_consumer_loop_breaks_on_connection_error():
    consumer = _StubAsyncCronConsumer(raise_after=0)
    await consumer._consumer_loop("melony_cron:default", consumer_id=0)
    assert "melony_cron:default" in consumer._initialized


async def test_async_consumer_loop_sleeps_between_iterations():
    consumer = _StubAsyncCronConsumer(raise_after=1)
    await consumer._consumer_loop("melony_cron:default", consumer_id=0)
    assert consumer._call_count == 2


async def test_async_consumer_loop_breaks_on_unexpected_exception():
    class _ErrorConsumer(_StubAsyncCronConsumer):
        async def _pop_due_entries(self, cron_queue: str):
            raise RuntimeError("unexpected")

    consumer = _ErrorConsumer()
    await consumer._consumer_loop("melony_cron:default", consumer_id=0)


async def test_async_consumer_loop_iteration_with_empty_entries():
    consumer = _StubAsyncCronConsumer()
    await consumer._consumer_loop_iteration("melony_cron:default", consumer_id=0)
    assert consumer._rescheduled == []


async def test_async_consumer_loop_iteration_executes_entries():
    entry = _make_entry()
    consumer = _StubAsyncCronConsumer(entries=[entry])
    with patch("melony.core.cron_consumers.find_task_func", return_value=lambda: None):
        await consumer._consumer_loop_iteration("melony_cron:default", consumer_id=0)
    assert len(consumer._rescheduled) == 1


async def test_async_execute_entry_success_sync_func():
    entry = _make_entry(retries=1, is_coro=False)
    consumer = _StubAsyncCronConsumer()
    with patch("melony.core.cron_consumers.find_task_func", return_value=lambda: None):
        await consumer._execute_entry(entry, "melony_cron:default", consumer_id=0)
    assert len(consumer._rescheduled) == 1


async def test_async_execute_entry_success_coro_func():
    async def _coro():
        pass

    entry = _make_entry(retries=1, is_coro=True)
    consumer = _StubAsyncCronConsumer()
    with patch("melony.core.cron_consumers.find_task_func", return_value=_coro):
        await consumer._execute_entry(entry, "melony_cron:default", consumer_id=0)
    assert len(consumer._rescheduled) == 1


async def test_async_execute_entry_failure_exhausts_retries():
    def _fail():
        raise ValueError("fail")

    entry = _make_entry(retries=2, is_coro=False)
    consumer = _StubAsyncCronConsumer()
    with patch("melony.core.cron_consumers.find_task_func", return_value=_fail):
        await consumer._execute_entry(entry, "melony_cron:default", consumer_id=0)
    assert len(consumer._rescheduled) == 1


async def test_async_execute_entry_failure_then_success():
    attempt = {"count": 0}

    def _sometimes_fail():
        attempt["count"] += 1
        if attempt["count"] < 2:
            raise ValueError("fail")

    entry = _make_entry(retries=3, is_coro=False)
    consumer = _StubAsyncCronConsumer()
    with patch("melony.core.cron_consumers.find_task_func", return_value=_sometimes_fail):
        await consumer._execute_entry(entry, "melony_cron:default", consumer_id=0)
    assert len(consumer._rescheduled) == 1


async def test_async_run_consumer_in_process_calls_asyncio_run():
    consumer = _StubAsyncCronConsumer(raise_after=0)
    with patch("asyncio.run") as mock_run:
        consumer._run_consumer_in_process("melony_cron:default", consumer_id=0)
        mock_run.assert_called_once()


def test_sync_start_consume_rejects_zero_processes():
    consumer = _StubSyncCronConsumer()
    with pytest.raises(ValueError, match="processes"):
        consumer.start_consume(processes=0)


def test_sync_start_consume_single_process_runs_loop():
    consumer = _StubSyncCronConsumer(raise_after=0)
    consumer.start_consume(queue="default", processes=1)
    assert "melony_cron:default" in consumer._initialized


def test_sync_start_consume_multiple_processes():
    consumer = _StubSyncCronConsumer(raise_after=0)
    with patch("multiprocessing.Process") as mock_proc_cls:
        mock_proc = MagicMock()
        mock_proc_cls.return_value = mock_proc
        with patch.object(consumer, "_run_all_processes") as mock_run:
            consumer.start_consume(processes=2)
            assert mock_proc_cls.call_count == 2
            mock_run.assert_called_once()


def test_sync_start_consume_terminates_on_keyboard_interrupt():
    consumer = _StubSyncCronConsumer(raise_after=0)
    with patch("multiprocessing.Process") as mock_proc_cls:
        mock_proc = MagicMock()
        mock_proc.is_alive.return_value = True
        mock_proc_cls.return_value = mock_proc
        with patch.object(consumer, "_run_all_processes", side_effect=KeyboardInterrupt):
            consumer.start_consume(processes=2)
        mock_proc.terminate.assert_called()


def test_sync_consumer_loop_breaks_on_connection_error():
    consumer = _StubSyncCronConsumer(raise_after=0)
    consumer._consumer_loop("melony_cron:default", consumer_id=0)
    assert "melony_cron:default" in consumer._initialized


def test_sync_consumer_loop_sleeps_between_iterations():
    consumer = _StubSyncCronConsumer(raise_after=1)
    consumer._consumer_loop("melony_cron:default", consumer_id=0)
    assert consumer._call_count == 2


def test_sync_consumer_loop_breaks_on_unexpected_exception():
    class _ErrorConsumer(_StubSyncCronConsumer):
        def _pop_due_entries(self, cron_queue: str):
            raise RuntimeError("unexpected")

    consumer = _ErrorConsumer()
    consumer._consumer_loop("melony_cron:default", consumer_id=0)


def test_sync_consumer_loop_iteration_with_empty_entries():
    consumer = _StubSyncCronConsumer()
    consumer._consumer_loop_iteration("melony_cron:default", consumer_id=0)
    assert consumer._rescheduled == []


def test_sync_consumer_loop_iteration_executes_entries():
    entry = _make_entry()
    consumer = _StubSyncCronConsumer(entries=[entry])
    with patch("melony.core.cron_consumers.find_task_func", return_value=lambda: None):
        consumer._consumer_loop_iteration("melony_cron:default", consumer_id=0)
    assert len(consumer._rescheduled) == 1


def test_sync_execute_entry_success():
    entry = _make_entry(retries=1, is_coro=False)
    consumer = _StubSyncCronConsumer()
    with patch("melony.core.cron_consumers.find_task_func", return_value=lambda: None):
        consumer._execute_entry(entry, "melony_cron:default", consumer_id=0)
    assert len(consumer._rescheduled) == 1


def test_sync_execute_entry_failure_exhausts_retries():
    def _fail():
        raise ValueError("fail")

    entry = _make_entry(retries=2, is_coro=False)
    consumer = _StubSyncCronConsumer()
    with patch("melony.core.cron_consumers.find_task_func", return_value=_fail):
        consumer._execute_entry(entry, "melony_cron:default", consumer_id=0)
    assert len(consumer._rescheduled) == 1


def test_sync_execute_entry_failure_then_success():
    attempt = {"count": 0}

    def _sometimes_fail():
        attempt["count"] += 1
        if attempt["count"] < 2:
            raise ValueError("fail")

    entry = _make_entry(retries=3, is_coro=False)
    consumer = _StubSyncCronConsumer()
    with patch("melony.core.cron_consumers.find_task_func", return_value=_sometimes_fail):
        consumer._execute_entry(entry, "melony_cron:default", consumer_id=0)
    assert len(consumer._rescheduled) == 1


def test_sync_run_consumer_in_process_calls_consumer_loop():
    consumer = _StubSyncCronConsumer(raise_after=0)
    with patch.object(consumer, "_consumer_loop") as mock_loop:
        consumer._run_consumer_in_process("melony_cron:default", consumer_id=0)
        mock_loop.assert_called_once_with(cron_queue="melony_cron:default", consumer_id=0)


def _make_registration(**kwargs) -> CronTaskRegistration:
    def dummy():
        pass

    defaults = dict(
        func_path="mymod.my_func",
        func=dummy,
        cron="* * * * *",
        queue="melony_cron:default",
        retries=1,
        retry_timeout=0,
        is_coro=False,
    )
    return CronTaskRegistration(**{**defaults, **kwargs})


async def test_async_pop_due_entries_returns_entries():
    entry = _make_entry()
    raw = entry.as_json().encode("utf-8")

    mock_script = AsyncMock(return_value=[raw])
    conn = AsyncMock()
    conn.register_script = MagicMock(return_value=mock_script)

    consumer = AsyncRedisCronConsumer(connection=conn, broker=MagicMock())
    entries = await consumer._pop_due_entries("melony_cron:default")

    assert len(entries) == 1
    assert entries[0].func_path == "mymod.my_func"


async def test_async_pop_due_entries_empty_returns_empty_list():
    mock_script = AsyncMock(return_value=[])
    conn = AsyncMock()
    conn.register_script = MagicMock(return_value=mock_script)

    consumer = AsyncRedisCronConsumer(connection=conn, broker=MagicMock())
    entries = await consumer._pop_due_entries("melony_cron:default")
    assert entries == []


async def test_async_initialize_cron_tasks_adds_to_sorted_set():
    conn = AsyncMock()
    reg = _make_registration(queue="melony_cron:default")
    broker = MagicMock()
    broker._cron_registry = [reg]

    consumer = AsyncRedisCronConsumer(connection=conn, broker=broker)
    await consumer._initialize_cron_tasks("melony_cron:default")

    conn.zadd.assert_called_once()


async def test_async_initialize_cron_tasks_skips_different_queue():
    conn = AsyncMock()
    reg = _make_registration(queue="melony_cron:other")
    broker = MagicMock()
    broker._cron_registry = [reg]

    consumer = AsyncRedisCronConsumer(connection=conn, broker=broker)
    await consumer._initialize_cron_tasks("melony_cron:default")

    conn.zadd.assert_not_called()


async def test_async_reschedule_adds_entry_with_next_run():
    conn = AsyncMock()
    entry = _make_entry()

    consumer = AsyncRedisCronConsumer(connection=conn, broker=MagicMock())
    await consumer._reschedule(entry, "melony_cron:default")

    conn.zadd.assert_called_once()
    assert conn.zadd.call_args[0][0] == "melony_cron:default"


def test_sync_pop_due_entries_returns_entries():
    entry = _make_entry()
    raw = entry.as_json().encode("utf-8")

    mock_script = MagicMock(return_value=[raw])
    conn = MagicMock()
    conn.register_script = MagicMock(return_value=mock_script)

    consumer = SyncRedisCronConsumer(connection=conn, broker=MagicMock())
    entries = consumer._pop_due_entries("melony_cron:default")

    assert len(entries) == 1
    assert entries[0].func_path == "mymod.my_func"


def test_sync_pop_due_entries_empty_returns_empty_list():
    mock_script = MagicMock(return_value=[])
    conn = MagicMock()
    conn.register_script = MagicMock(return_value=mock_script)

    consumer = SyncRedisCronConsumer(connection=conn, broker=MagicMock())
    entries = consumer._pop_due_entries("melony_cron:default")
    assert entries == []


def test_sync_initialize_cron_tasks_adds_to_sorted_set():
    conn = MagicMock()
    reg = _make_registration(queue="melony_cron:default")
    broker = MagicMock()
    broker._cron_registry = [reg]

    consumer = SyncRedisCronConsumer(connection=conn, broker=broker)
    consumer._initialize_cron_tasks("melony_cron:default")

    conn.zadd.assert_called_once()


def test_sync_initialize_cron_tasks_skips_different_queue():
    conn = MagicMock()
    reg = _make_registration(queue="melony_cron:other")
    broker = MagicMock()
    broker._cron_registry = [reg]

    consumer = SyncRedisCronConsumer(connection=conn, broker=broker)
    consumer._initialize_cron_tasks("melony_cron:default")

    conn.zadd.assert_not_called()


def test_sync_reschedule_adds_entry_with_next_run():
    conn = MagicMock()
    entry = _make_entry()

    consumer = SyncRedisCronConsumer(connection=conn, broker=MagicMock())
    consumer._reschedule(entry, "melony_cron:default")

    conn.zadd.assert_called_once()
    assert conn.zadd.call_args[0][0] == "melony_cron:default"
