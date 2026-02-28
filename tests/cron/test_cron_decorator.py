import pytest
from unittest.mock import MagicMock

from melony.core.brokers import BaseBroker
from melony.core.task_wrappers import AsyncTaskWrapper, SyncTaskWrapper


class _StubBroker(BaseBroker):
    @property
    def publisher(self):
        return MagicMock()

    @property
    def consumer(self):
        return MagicMock()

    @property
    def cron_consumer(self):
        return MagicMock()


def test_cron_task_registered_in_registry():
    broker = _StubBroker()

    @broker.task(cron="* * * * *")
    def my_task():
        pass

    assert len(broker._cron_registry) == 1


def test_cron_task_stores_correct_cron_expression():
    broker = _StubBroker()

    @broker.task(cron="0 8 * * 1-5")
    def my_task():
        pass

    assert broker._cron_registry[0].cron == "0 8 * * 1-5"


def test_cron_task_is_coro_false_for_sync():
    broker = _StubBroker()

    @broker.task(cron="* * * * *")
    def sync_task():
        pass

    assert broker._cron_registry[0].is_coro is False


def test_cron_task_is_coro_true_for_async():
    broker = _StubBroker()

    @broker.task(cron="* * * * *")
    async def async_task():
        pass

    assert broker._cron_registry[0].is_coro is True


def test_cron_task_default_queue():
    broker = _StubBroker()

    @broker.task(cron="* * * * *")
    def my_task():
        pass

    assert broker._cron_registry[0].queue == "melony_cron:default"


def test_cron_task_custom_queue():
    broker = _StubBroker()

    @broker.task(cron="* * * * *", queue="reports")
    def my_task():
        pass

    assert broker._cron_registry[0].queue == "melony_cron:reports"


def test_cron_task_stores_retries_and_timeout():
    broker = _StubBroker()

    @broker.task(cron="* * * * *", retries=5, retry_timeout=30)
    def my_task():
        pass

    reg = broker._cron_registry[0]
    assert reg.retries == 5
    assert reg.retry_timeout == 30


def test_cron_task_stores_original_func():
    broker = _StubBroker()

    def original():
        return "result"

    broker.task(cron="* * * * *")(original)

    assert broker._cron_registry[0].func() == "result"


def test_cron_task_func_path_contains_qualname():
    broker = _StubBroker()

    @broker.task(cron="* * * * *")
    def my_task():
        pass

    assert "my_task" in broker._cron_registry[0].func_path


def test_multiple_cron_tasks_all_registered():
    broker = _StubBroker()

    @broker.task(cron="* * * * *")
    def task_one():
        pass

    @broker.task(cron="0 12 * * *")
    def task_two():
        pass

    assert len(broker._cron_registry) == 2
    crons = {r.cron for r in broker._cron_registry}
    assert crons == {"* * * * *", "0 12 * * *"}


def test_no_cron_does_not_register():
    broker = _StubBroker()

    @broker.task
    def my_task():
        pass

    assert len(broker._cron_registry) == 0


def test_no_cron_with_params_does_not_register():
    broker = _StubBroker()

    @broker.task(retries=3)
    def my_task():
        pass

    assert len(broker._cron_registry) == 0


def test_cron_sync_task_still_returns_sync_wrapper():
    broker = _StubBroker()

    @broker.task(cron="* * * * *")
    def my_task():
        pass

    assert isinstance(my_task(), SyncTaskWrapper)


def test_cron_async_task_still_returns_async_wrapper():
    broker = _StubBroker()

    @broker.task(cron="* * * * *")
    async def my_task():
        pass

    assert isinstance(my_task(), AsyncTaskWrapper)


def test_invalid_cron_raises_value_error():
    broker = _StubBroker()

    with pytest.raises(ValueError, match="Invalid cron expression"):
        @broker.task(cron="not-a-cron")
        def my_task():
            pass


def test_invalid_cron_five_asterisks_is_valid():
    broker = _StubBroker()

    @broker.task(cron="* * * * *")
    def my_task():
        pass

    assert len(broker._cron_registry) == 1


def test_zero_retries_raises_value_error():
    broker = _StubBroker()

    with pytest.raises(ValueError, match="retries"):
        @broker.task(cron="* * * * *", retries=0)
        def my_task():
            pass


def test_negative_retry_timeout_raises_value_error():
    broker = _StubBroker()

    with pytest.raises(ValueError, match="retry_timeout"):
        @broker.task(cron="* * * * *", retry_timeout=-1)
        def my_task():
            pass
