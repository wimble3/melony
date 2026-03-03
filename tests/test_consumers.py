import time

import pytest

from unittest.mock import MagicMock, patch
from redis.exceptions import ConnectionError

from melony.core.consumers import BaseAsyncConsumer, BaseConsumer, BaseSyncConsumer
from melony.core.dto import TaskExecResultsDTO
from melony.core.publishers import IAsyncPublisher, ISyncPublisher
from redis.asyncio import Redis as AsyncRedis
from redis import Redis as SyncRedis


class _StubAsyncPublisher(IAsyncPublisher):
    def __init__(self):
        self._pushed = []
        self._conn = MagicMock()

    @property
    def connection(self) -> AsyncRedis:
        return self._conn

    async def push(self, task) -> None:
        self._pushed.append(task)


class _StubSyncPublisher(ISyncPublisher):
    def __init__(self):
        self._pushed = []
        self._conn = MagicMock()

    @property
    def connection(self) -> SyncRedis:
        return self._conn

    def push(self, task) -> None:
        self._pushed.append(task)


class _BreakingAsyncConsumer(BaseAsyncConsumer):
    """Breaks the loop immediately by raising ConnectionError from _pop_tasks."""

    async def _pop_tasks(self, queue: str):
        raise ConnectionError("stopped")


class _EmptyAsyncConsumer(BaseAsyncConsumer):
    """Returns one empty batch, then raises ConnectionError to stop."""

    def __init__(self, publisher, batches=None):
        super().__init__(publisher)
        self._batches = list(batches or [])

    async def _pop_tasks(self, queue: str):
        if self._batches:
            return self._batches.pop(0)
        raise ConnectionError("done")


class _BreakingSyncConsumer(BaseSyncConsumer):
    """Breaks the loop immediately."""

    def _pop_tasks(self, queue: str):
        raise ConnectionError("stopped")


def test_run_all_processes_calls_start_and_join_on_each():
    consumer = BaseConsumer()
    p1, p2 = MagicMock(), MagicMock()
    consumer._run_all_processes([p1, p2])
    p1.start.assert_called_once()
    p1.join.assert_called_once()
    p2.start.assert_called_once()
    p2.join.assert_called_once()


def test_filter_tasks_past_task_is_ready_to_execute():
    consumer = BaseConsumer()
    past_task = MagicMock()
    past_task.get_execution_timestamp.return_value = time.time() - 100
    result = consumer._filter_tasks_by_execution_time([past_task], consumer_id=0)
    assert list(result.tasks_to_execute) == [past_task]
    assert list(result.tasks_to_push_back) == []


def test_filter_tasks_future_task_is_pushed_back():
    consumer = BaseConsumer()
    future_task = MagicMock()
    future_task.get_execution_timestamp.return_value = time.time() + 100
    result = consumer._filter_tasks_by_execution_time([future_task], consumer_id=0)
    assert list(result.tasks_to_execute) == []
    assert list(result.tasks_to_push_back) == [future_task]


def test_filter_tasks_mixed_batch_splits_correctly():
    consumer = BaseConsumer()
    past = MagicMock()
    past.get_execution_timestamp.return_value = time.time() - 1
    future = MagicMock()
    future.get_execution_timestamp.return_value = time.time() + 100
    result = consumer._filter_tasks_by_execution_time([past, future], consumer_id=0)
    assert list(result.tasks_to_execute) == [past]
    assert list(result.tasks_to_push_back) == [future]


def test_async_consumer_init_sets_connection():
    publisher = _StubAsyncPublisher()
    consumer = _BreakingAsyncConsumer(publisher)
    assert consumer._connection is publisher.connection


def test_async_consumer_init_with_result_backend_asserts_saver_type():
    from melony.core.result_backends import IAsyncResultBackendSaver, IResultBackend

    class _FakeAsyncSaver(IAsyncResultBackendSaver):
        async def save_results(self, task_results) -> None:
            pass

    class _FakeResultBackend(IResultBackend):
        @property
        def saver(self):
            return _FakeAsyncSaver()

    publisher = _StubAsyncPublisher()
    consumer = _BreakingAsyncConsumer(publisher, result_backend=_FakeResultBackend())
    assert consumer._result_backend is not None


async def test_async_consumer_start_consume_rejects_zero_processes():
    consumer = _BreakingAsyncConsumer(_StubAsyncPublisher())
    with pytest.raises(ValueError, match="processes"):
        await consumer.start_consume(processes=0)


async def test_async_consumer_start_consume_single_process_runs_loop():
    consumer = _BreakingAsyncConsumer(_StubAsyncPublisher())
    await consumer.start_consume(queue="default", processes=1)


async def test_async_consumer_loop_breaks_on_connection_error():
    consumer = _BreakingAsyncConsumer(_StubAsyncPublisher())
    await consumer._consumer_loop("melony_tasks:default", consumer_id=0)


async def test_async_consumer_loop_breaks_on_unexpected_exception():
    class _ErrorConsumer(BaseAsyncConsumer):
        async def _pop_tasks(self, queue: str):
            raise RuntimeError("unexpected")

    consumer = _ErrorConsumer(_StubAsyncPublisher())
    await consumer._consumer_loop("melony_tasks:default", consumer_id=0)


async def test_async_consumer_loop_iteration_with_empty_batch():
    consumer = _EmptyAsyncConsumer(_StubAsyncPublisher(), batches=[[]])
    await consumer._consumer_loop_iteration("melony_tasks:default", consumer_id=0)


async def test_async_push_bulk_calls_publisher_push():
    publisher = _StubAsyncPublisher()
    consumer = _BreakingAsyncConsumer(publisher)
    mock_task = MagicMock()
    await consumer._push_bulk([mock_task])
    assert mock_task in publisher._pushed


async def test_async_retry_policy_pushes_back_task_with_retries_left():
    publisher = _StubAsyncPublisher()
    consumer = _BreakingAsyncConsumer(publisher)

    task = MagicMock()
    task._meta.retries_left = 2
    task.retry_timeout = 0
    task._meta.timestamp = 1000.0

    result = TaskExecResultsDTO(tasks_with_result=[], tasks_to_retry=[task])
    await consumer._retry_policy(result)
    assert task in publisher._pushed


async def test_async_retry_policy_adjusts_timestamp_when_retry_timeout_set():
    publisher = _StubAsyncPublisher()
    consumer = _BreakingAsyncConsumer(publisher)

    task = MagicMock()
    task._meta.retries_left = 1
    task.retry_timeout = 30
    task._meta.timestamp = 1000.0

    result = TaskExecResultsDTO(tasks_with_result=[], tasks_to_retry=[task])
    await consumer._retry_policy(result)
    assert task._meta.timestamp == 1030.0


async def test_async_retry_policy_skips_task_with_no_retries_left():
    publisher = _StubAsyncPublisher()
    consumer = _BreakingAsyncConsumer(publisher)

    task = MagicMock()
    task._meta.retries_left = 0

    result = TaskExecResultsDTO(tasks_with_result=[], tasks_to_retry=[task])
    await consumer._retry_policy(result)
    assert task not in publisher._pushed


async def test_async_consumer_start_consume_multiple_processes():
    consumer = _BreakingAsyncConsumer(_StubAsyncPublisher())
    with patch("multiprocessing.Process") as mock_proc_cls:
        mock_proc = MagicMock()
        mock_proc_cls.return_value = mock_proc
        with patch.object(consumer, "_run_all_processes") as mock_run:
            await consumer.start_consume(processes=2)
            assert mock_proc_cls.call_count == 2
            mock_run.assert_called_once()


async def test_async_consumer_start_consume_terminates_on_keyboard_interrupt():
    consumer = _BreakingAsyncConsumer(_StubAsyncPublisher())
    with patch("multiprocessing.Process") as mock_proc_cls:
        mock_proc = MagicMock()
        mock_proc.is_alive.return_value = True
        mock_proc_cls.return_value = mock_proc
        with patch.object(consumer, "_run_all_processes", side_effect=KeyboardInterrupt):
            await consumer.start_consume(processes=2)
        mock_proc.terminate.assert_called()


def test_async_run_consumer_in_process_calls_asyncio_run():
    consumer = _BreakingAsyncConsumer(_StubAsyncPublisher())
    with patch("asyncio.run") as mock_run:
        consumer._run_consumer_in_process("melony_tasks:default", consumer_id=0)
        mock_run.assert_called_once()


def test_sync_consumer_init_sets_connection():
    publisher = _StubSyncPublisher()
    consumer = _BreakingSyncConsumer(publisher)
    assert consumer._connection is publisher.connection


def test_sync_consumer_init_with_result_backend_asserts_saver_type():
    from melony.core.result_backends import ISyncResultBackendSaver, IResultBackend

    class _FakeSyncSaver(ISyncResultBackendSaver):
        def save_results(self, task_results) -> None:
            pass

    class _FakeResultBackend(IResultBackend):
        @property
        def saver(self):
            return _FakeSyncSaver()

    publisher = _StubSyncPublisher()
    consumer = _BreakingSyncConsumer(publisher, result_backend=_FakeResultBackend())
    assert consumer._result_backend is not None


def test_sync_consumer_start_consume_rejects_zero_processes():
    consumer = _BreakingSyncConsumer(_StubSyncPublisher())
    with pytest.raises(ValueError, match="processes"):
        consumer.start_consume(processes=0)


def test_sync_consumer_start_consume_single_process_runs_loop():
    consumer = _BreakingSyncConsumer(_StubSyncPublisher())
    consumer.start_consume(queue="default", processes=1)


def test_sync_consumer_loop_breaks_on_connection_error():
    consumer = _BreakingSyncConsumer(_StubSyncPublisher())
    consumer._consumer_loop("melony_tasks:default", consumer_id=0)


def test_sync_consumer_loop_breaks_on_unexpected_exception():
    class _ErrorConsumer(BaseSyncConsumer):
        def _pop_tasks(self, queue: str):
            raise RuntimeError("unexpected")

    consumer = _ErrorConsumer(_StubSyncPublisher())
    consumer._consumer_loop("melony_tasks:default", consumer_id=0)


def test_sync_consumer_loop_iteration_with_empty_batch():
    class _EmptySyncConsumer(BaseSyncConsumer):
        def __init__(self, publisher):
            super().__init__(publisher)
            self._called = False

        def _pop_tasks(self, queue: str):
            if not self._called:
                self._called = True
                return []
            raise ConnectionError("done")

    publisher = _StubSyncPublisher()
    consumer = _EmptySyncConsumer(publisher)
    consumer._consumer_loop_iteration("melony_tasks:default", consumer_id=0)


def test_sync_push_bulk_calls_publisher_push():
    publisher = _StubSyncPublisher()
    consumer = _BreakingSyncConsumer(publisher)
    mock_task = MagicMock()
    consumer._push_bulk([mock_task])
    assert mock_task in publisher._pushed


def test_sync_retry_policy_pushes_back_task_with_retries_left():
    publisher = _StubSyncPublisher()
    consumer = _BreakingSyncConsumer(publisher)

    task = MagicMock()
    task._meta.retries_left = 2
    task.retry_timeout = 0
    task._meta.timestamp = 1000.0

    result = TaskExecResultsDTO(tasks_with_result=[], tasks_to_retry=[task])
    consumer._retry_policy(result)
    assert task in publisher._pushed


def test_sync_retry_policy_adjusts_timestamp_when_retry_timeout_set():
    publisher = _StubSyncPublisher()
    consumer = _BreakingSyncConsumer(publisher)

    task = MagicMock()
    task._meta.retries_left = 1
    task.retry_timeout = 15
    task._meta.timestamp = 500.0

    result = TaskExecResultsDTO(tasks_with_result=[], tasks_to_retry=[task])
    consumer._retry_policy(result)
    assert task._meta.timestamp == 515.0


def test_sync_retry_policy_skips_task_with_no_retries_left():
    publisher = _StubSyncPublisher()
    consumer = _BreakingSyncConsumer(publisher)

    task = MagicMock()
    task._meta.retries_left = 0

    result = TaskExecResultsDTO(tasks_with_result=[], tasks_to_retry=[task])
    consumer._retry_policy(result)
    assert task not in publisher._pushed


def test_sync_consumer_start_consume_multiple_processes():
    consumer = _BreakingSyncConsumer(_StubSyncPublisher())
    with patch("multiprocessing.Process") as mock_proc_cls:
        mock_proc = MagicMock()
        mock_proc_cls.return_value = mock_proc
        consumer.start_consume(processes=2)
        assert mock_proc.start.called
