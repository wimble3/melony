import json

import pytest

from melony.core.cron_tasks import CronEntry, CronTaskRegistration


def _make_entry(**kwargs) -> CronEntry:
    defaults = dict(
        func_path="mymod.my_func",
        cron="* * * * *",
        queue="melony_cron:default",
        retries=3,
        retry_timeout=0,
        is_coro=False,
    )
    return CronEntry(**{**defaults, **kwargs})


def test_as_json_produces_valid_json():
    entry = _make_entry()
    data = json.loads(entry.as_json())
    assert data["func_path"] == "mymod.my_func"
    assert data["cron"] == "* * * * *"
    assert data["queue"] == "melony_cron:default"
    assert data["retries"] == 3
    assert data["retry_timeout"] == 0
    assert data["is_coro"] is False


def test_from_json_round_trip():
    entry = _make_entry(is_coro=True, retry_timeout=30, retries=5)
    assert CronEntry.from_json(entry.as_json()) == entry


def test_from_json_restores_all_fields():
    entry = _make_entry(func_path="tasks.send_email", cron="0 9 * * 1", retries=2)
    restored = CronEntry.from_json(entry.as_json())
    assert restored.func_path == entry.func_path
    assert restored.cron == entry.cron
    assert restored.queue == entry.queue
    assert restored.retries == entry.retries
    assert restored.retry_timeout == entry.retry_timeout
    assert restored.is_coro == entry.is_coro


def test_from_registration_maps_all_fields():
    def my_func():
        pass

    reg = CronTaskRegistration(
        func_path="tasks.my_func",
        func=my_func,
        cron="0 12 * * *",
        queue="melony_cron:reports",
        retries=2,
        retry_timeout=10,
        is_coro=False,
    )
    entry = CronEntry.from_registration(reg)

    assert entry.func_path == reg.func_path
    assert entry.cron == reg.cron
    assert entry.queue == reg.queue
    assert entry.retries == reg.retries
    assert entry.retry_timeout == reg.retry_timeout
    assert entry.is_coro == reg.is_coro


def test_from_registration_does_not_include_func():
    def my_func():
        pass

    reg = CronTaskRegistration(
        func_path="m.f", func=my_func, cron="* * * * *",
        queue="melony_cron:default", retries=1, retry_timeout=0, is_coro=False,
    )
    entry = CronEntry.from_registration(reg)
    assert not hasattr(entry, "func")


def test_from_registration_is_coro_true():
    async def my_coro():
        pass

    reg = CronTaskRegistration(
        func_path="m.my_coro", func=my_coro, cron="* * * * *",
        queue="melony_cron:default", retries=1, retry_timeout=0, is_coro=True,
    )
    entry = CronEntry.from_registration(reg)
    assert entry.is_coro is True
