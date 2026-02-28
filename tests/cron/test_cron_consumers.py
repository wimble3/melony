import pytest

from unittest.mock import MagicMock, patch
from redis.exceptions import ConnectionError

from melony.core.cron_consumers import BaseAsyncCronConsumer, BaseSyncCronConsumer
from melony.core.cron_tasks import CronEntry


_QUEUE = "melony_cron:default"


def _make_entry(**kwargs) -> CronEntry:
    defaults = dict(
        func_path="mymod.my_func",
        cron="* * * * *",
        queue=_QUEUE,
        retries=3,
        retry_timeout=0,
        is_coro=False,
    )
    return CronEntry(**{**defaults, **kwargs})


class _AsyncStub(BaseAsyncCronConsumer):
    def __init__(self, broker=None, due_batches=None):
        super().__init__(connection=None, broker=broker or MagicMock())
        self._due_batches: list[list[CronEntry]] = due_batches or []
        self.rescheduled: list[CronEntry] = []
        self.initialized: list[str] = []

    async def _pop_due_entries(self, cron_queue: str) -> list[CronEntry]:
        if self._due_batches:
            return self._due_batches.pop(0)
        return []

    async def _initialize_cron_tasks(self, cron_queue: str) -> None:
        self.initialized.append(cron_queue)

    async def _reschedule(self, entry: CronEntry, cron_queue: str) -> None:
        self.rescheduled.append(entry)


class _SyncStub(BaseSyncCronConsumer):
    def __init__(self, broker=None, due_batches=None):
        super().__init__(connection=None, broker=broker or MagicMock())
        self._due_batches: list[list[CronEntry]] = due_batches or []
        self.rescheduled: list[CronEntry] = []
        self.initialized: list[str] = []

    def _pop_due_entries(self, cron_queue: str) -> list[CronEntry]:
        if self._due_batches:
            return self._due_batches.pop(0)
        return []

    def _initialize_cron_tasks(self, cron_queue: str) -> None:
        self.initialized.append(cron_queue)

    def _reschedule(self, entry: CronEntry, cron_queue: str) -> None:
        self.rescheduled.append(entry)


async def test_async_start_consume_rejects_zero_processes():
    consumer = _AsyncStub()
    with pytest.raises(ValueError, match="processes"):
        await consumer.start_consume(processes=0)


async def test_async_start_consume_rejects_negative_processes():
    consumer = _AsyncStub()
    with pytest.raises(ValueError):
        await consumer.start_consume(processes=-5)


def test_sync_start_consume_rejects_zero_processes():
    consumer = _SyncStub()
    with pytest.raises(ValueError, match="processes"):
        consumer.start_consume(processes=0)


async def test_async_execute_entry_success_first_attempt():
    call_count = 0

    async def my_func():
        nonlocal call_count
        call_count += 1

    entry = _make_entry(is_coro=True)
    consumer = _AsyncStub()

    with patch("melony.core.cron_consumers.find_task_func", return_value=my_func):
        await consumer._execute_entry(entry, _QUEUE, 0)

    assert call_count == 1
    assert consumer.rescheduled == [entry]


async def test_async_execute_entry_retries_until_success():
    call_count = 0

    async def flaky():
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise RuntimeError("temporary failure")

    entry = _make_entry(is_coro=True, retries=3)
    consumer = _AsyncStub()

    with patch("melony.core.cron_consumers.find_task_func", return_value=flaky):
        await consumer._execute_entry(entry, _QUEUE, 0)

    assert call_count == 3
    assert len(consumer.rescheduled) == 1


async def test_async_execute_entry_reschedules_after_all_retries_exhausted():
    async def always_fails():
        raise ValueError("boom")

    entry = _make_entry(is_coro=True, retries=3)
    consumer = _AsyncStub()

    with patch("melony.core.cron_consumers.find_task_func", return_value=always_fails):
        await consumer._execute_entry(entry, _QUEUE, 0)

    assert len(consumer.rescheduled) == 1


async def test_async_execute_entry_exhausts_all_retries():
    call_count = 0

    async def always_fails():
        nonlocal call_count
        call_count += 1
        raise ValueError("boom")

    entry = _make_entry(is_coro=True, retries=5)
    consumer = _AsyncStub()

    with patch("melony.core.cron_consumers.find_task_func", return_value=always_fails):
        await consumer._execute_entry(entry, _QUEUE, 0)

    assert call_count == 5


async def test_async_execute_entry_calls_sync_func_directly():
    call_count = 0

    def sync_func():
        nonlocal call_count
        call_count += 1

    entry = _make_entry(is_coro=False)
    consumer = _AsyncStub()

    with patch("melony.core.cron_consumers.find_task_func", return_value=sync_func):
        await consumer._execute_entry(entry, _QUEUE, 0)

    assert call_count == 1


async def test_async_execute_entry_sleeps_between_failed_attempts():
    async def always_fails():
        raise RuntimeError()

    entry = _make_entry(is_coro=True, retries=3, retry_timeout=5)
    consumer = _AsyncStub()

    with patch("melony.core.cron_consumers.find_task_func", return_value=always_fails):
        with patch("asyncio.sleep") as mock_sleep:
            await consumer._execute_entry(entry, _QUEUE, 0)

    assert mock_sleep.call_count == 2
    mock_sleep.assert_called_with(5)


async def test_async_execute_entry_no_sleep_after_last_attempt():
    async def always_fails():
        raise RuntimeError()

    entry = _make_entry(is_coro=True, retries=1, retry_timeout=10)
    consumer = _AsyncStub()

    with patch("melony.core.cron_consumers.find_task_func", return_value=always_fails):
        with patch("asyncio.sleep") as mock_sleep:
            await consumer._execute_entry(entry, _QUEUE, 0)

    assert mock_sleep.call_count == 0


async def test_async_execute_entry_no_sleep_on_success():
    async def my_func():
        pass

    entry = _make_entry(is_coro=True, retries=3, retry_timeout=10)
    consumer = _AsyncStub()

    with patch("melony.core.cron_consumers.find_task_func", return_value=my_func):
        with patch("asyncio.sleep") as mock_sleep:
            await consumer._execute_entry(entry, _QUEUE, 0)

    assert mock_sleep.call_count == 0


def test_sync_execute_entry_success_first_attempt():
    call_count = 0

    def my_func():
        nonlocal call_count
        call_count += 1

    entry = _make_entry(is_coro=False)
    consumer = _SyncStub()

    with patch("melony.core.cron_consumers.find_task_func", return_value=my_func):
        consumer._execute_entry(entry, _QUEUE, 0)

    assert call_count == 1
    assert consumer.rescheduled == [entry]


def test_sync_execute_entry_retries_until_success():
    call_count = 0

    def flaky():
        nonlocal call_count
        call_count += 1
        if call_count < 2:
            raise RuntimeError("fail once")

    entry = _make_entry(is_coro=False, retries=3)
    consumer = _SyncStub()

    with patch("melony.core.cron_consumers.find_task_func", return_value=flaky):
        consumer._execute_entry(entry, _QUEUE, 0)

    assert call_count == 2
    assert len(consumer.rescheduled) == 1


def test_sync_execute_entry_reschedules_after_all_retries_exhausted():
    def always_fails():
        raise ValueError()

    entry = _make_entry(is_coro=False, retries=3)
    consumer = _SyncStub()

    with patch("melony.core.cron_consumers.find_task_func", return_value=always_fails):
        consumer._execute_entry(entry, _QUEUE, 0)

    assert len(consumer.rescheduled) == 1


def test_sync_execute_entry_exhausts_all_retries():
    call_count = 0

    def always_fails():
        nonlocal call_count
        call_count += 1
        raise ValueError()

    entry = _make_entry(is_coro=False, retries=4)
    consumer = _SyncStub()

    with patch("melony.core.cron_consumers.find_task_func", return_value=always_fails):
        consumer._execute_entry(entry, _QUEUE, 0)

    assert call_count == 4


def test_sync_execute_entry_sleeps_between_failed_attempts():
    def always_fails():
        raise RuntimeError()

    entry = _make_entry(is_coro=False, retries=3, retry_timeout=10)
    consumer = _SyncStub()

    with patch("melony.core.cron_consumers.find_task_func", return_value=always_fails):
        with patch("time.sleep") as mock_sleep:
            consumer._execute_entry(entry, _QUEUE, 0)

    assert mock_sleep.call_count == 2
    mock_sleep.assert_called_with(10)


def test_sync_execute_entry_no_sleep_after_last_attempt():
    def always_fails():
        raise RuntimeError()

    entry = _make_entry(is_coro=False, retries=1, retry_timeout=10)
    consumer = _SyncStub()

    with patch("melony.core.cron_consumers.find_task_func", return_value=always_fails):
        with patch("time.sleep") as mock_sleep:
            consumer._execute_entry(entry, _QUEUE, 0)

    assert mock_sleep.call_count == 0


async def test_async_loop_iteration_executes_all_due_entries():
    entry1 = _make_entry(func_path="m.f1", is_coro=True)
    entry2 = _make_entry(func_path="m.f2", is_coro=True)

    async def noop():
        pass

    consumer = _AsyncStub(due_batches=[[entry1, entry2]])

    with patch("melony.core.cron_consumers.find_task_func", return_value=noop):
        await consumer._consumer_loop_iteration(_QUEUE, 0)

    assert consumer.rescheduled == [entry1, entry2]


async def test_async_loop_iteration_empty_batch_no_reschedule():
    consumer = _AsyncStub(due_batches=[[]])
    await consumer._consumer_loop_iteration(_QUEUE, 0)
    assert consumer.rescheduled == []


def test_sync_loop_iteration_executes_all_due_entries():
    entry1 = _make_entry(func_path="m.f1")
    entry2 = _make_entry(func_path="m.f2")

    def noop():
        pass

    consumer = _SyncStub(due_batches=[[entry1, entry2]])

    with patch("melony.core.cron_consumers.find_task_func", return_value=noop):
        consumer._consumer_loop_iteration(_QUEUE, 0)

    assert consumer.rescheduled == [entry1, entry2]


async def test_async_consumer_loop_breaks_on_connection_error():
    class _ErrorStub(_AsyncStub):
        async def _pop_due_entries(self, cron_queue: str) -> list[CronEntry]:
            raise ConnectionError("Redis down")

    consumer = _ErrorStub()
    await consumer._consumer_loop(_QUEUE, 0)


async def test_async_consumer_loop_breaks_on_unexpected_exception():
    class _ErrorStub(_AsyncStub):
        async def _pop_due_entries(self, cron_queue: str) -> list[CronEntry]:
            raise RuntimeError("unexpected")

    consumer = _ErrorStub()
    await consumer._consumer_loop(_QUEUE, 0)


async def test_async_consumer_loop_initializes_before_iterating():
    class _ErrorStub(_AsyncStub):
        async def _pop_due_entries(self, cron_queue: str) -> list[CronEntry]:
            raise ConnectionError()

    consumer = _ErrorStub()
    await consumer._consumer_loop("melony_cron:reports", 0)

    assert "melony_cron:reports" in consumer.initialized


def test_sync_consumer_loop_breaks_on_connection_error():
    class _ErrorStub(_SyncStub):
        def _pop_due_entries(self, cron_queue: str) -> list[CronEntry]:
            raise ConnectionError("Redis down")

    consumer = _ErrorStub()
    consumer._consumer_loop(_QUEUE, 0)


def test_sync_consumer_loop_breaks_on_unexpected_exception():
    class _ErrorStub(_SyncStub):
        def _pop_due_entries(self, cron_queue: str) -> list[CronEntry]:
            raise RuntimeError("unexpected")

    consumer = _ErrorStub()
    consumer._consumer_loop(_QUEUE, 0)


def test_sync_consumer_loop_initializes_before_iterating():
    class _ErrorStub(_SyncStub):
        def _pop_due_entries(self, cron_queue: str) -> list[CronEntry]:
            raise ConnectionError()

    consumer = _ErrorStub()
    consumer._consumer_loop("melony_cron:emails", 0)

    assert "melony_cron:emails" in consumer.initialized
