import asyncio
import multiprocessing
import time

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Final, final
from redis.exceptions import ConnectionError

from melony.core.consts import CRON_QUEUE_PREFIX, DEFAULT_QUEUE
from melony.core.cron_tasks import CronEntry
from melony.core.task_finders import find_task_func
from melony.logger import log_error, log_info

if TYPE_CHECKING:
    from melony.core.brokers import BaseBroker
    from melony.core.result_backends import IResultBackend

__all__ = ()

_CRON_COMPLETE_MSG: Final[str] = "Cron task '{}' completed"
_CRON_FAILED_MSG: Final[str] = "Cron task '{}' failed on attempt {}"


class BaseCronConsumer:
    @final
    def _run_all_processes(self, processes: list) -> None:
        for process in processes:
            process.start()
        for process in processes:
            process.join()


class BaseAsyncCronConsumer(ABC, BaseCronConsumer):  # noqa: WPS214
    _poll_interval_secs: Final[int] = 1

    def __init__(
        self,
        connection,
        broker: "BaseBroker",
        result_backend: "IResultBackend | None" = None,
    ) -> None:
        self._connection = connection
        self._broker = broker
        self._result_backend = result_backend

    @final
    async def start_consume(
        self,
        queue: str = DEFAULT_QUEUE,
        processes: int = 1,
    ) -> None:
        cron_queue = f"{CRON_QUEUE_PREFIX}{queue}"
        if processes < 1:
            raise ValueError("Param 'processes' must be positive integer (without zero)")
        if processes == 1:
            await self._consumer_loop(cron_queue=cron_queue, consumer_id=0)
            return
        running_processes = []
        for process_num in range(processes):
            process = multiprocessing.Process(
                name=f"melony-cron-process-{process_num}",
                target=self._run_consumer_in_process,
                args=(cron_queue, process_num),
                daemon=False,
            )
            running_processes.append(process)
        try:
            self._run_all_processes(running_processes)
        except KeyboardInterrupt:
            for process in running_processes:
                if process.is_alive():
                    process.terminate()

    @final
    async def _consumer_loop(self, cron_queue: str, consumer_id: int = 0) -> None:
        await self._initialize_cron_tasks(cron_queue)
        log_info(f"Start listening cron queue {cron_queue}", consumer_id=consumer_id)
        while True:
            try:
                await self._consumer_loop_iteration(cron_queue, consumer_id)
            except ConnectionError as exc:
                log_error("Redis connection error", exc=exc)
                break
            except Exception as exc:
                log_error("Unexpected error at cron consuming loop", exc=exc)
                break
            await asyncio.sleep(self._poll_interval_secs)

    @final
    async def _consumer_loop_iteration(
        self,
        cron_queue: str,
        consumer_id: int,
    ) -> None:
        pop_due = await self._pop_due_entries(cron_queue)
        for entry in pop_due:
            await self._execute_entry(entry, cron_queue, consumer_id)  # noqa: WPS476

    @final
    async def _execute_entry(  # noqa: WPS231
        self,
        cron_entry: CronEntry,
        cron_queue: str,
        consumer_id: int,
    ) -> None:
        func = find_task_func(cron_entry.func_path)
        remaining = cron_entry.retries
        attempt_num = 0
        while remaining > 0:
            remaining -= 1
            attempt_num += 1
            try:
                if cron_entry.is_coro:
                    await func()
                else:
                    func()
            except Exception as exc:
                log_error(
                    _CRON_FAILED_MSG.format(cron_entry.func_path, attempt_num),
                    exc=exc,
                    consumer_id=consumer_id,
                )
                if remaining > 0:
                    await asyncio.sleep(cron_entry.retry_timeout)
                continue
            log_info(_CRON_COMPLETE_MSG.format(cron_entry.func_path), consumer_id=consumer_id)
            break
        await self._reschedule(cron_entry, cron_queue)

    @final
    def _run_consumer_in_process(self, cron_queue: str, consumer_id: int = 0) -> None:
        asyncio.run(self._consumer_loop(cron_queue=cron_queue, consumer_id=consumer_id))

    @abstractmethod
    async def _pop_due_entries(self, cron_queue: str) -> list[CronEntry]:
        """Atomically pop all due cron entries from the sorted set."""

    @abstractmethod
    async def _initialize_cron_tasks(self, cron_queue: str) -> None:
        """Initialize cron tasks."""

    @abstractmethod
    async def _reschedule(self, entry: CronEntry, cron_queue: str) -> None:
        """Reschedule a cron entry with its next execution time."""


class BaseSyncCronConsumer(ABC, BaseCronConsumer):  # noqa: WPS214
    _poll_interval_secs: Final[int] = 1

    def __init__(
        self,
        connection,
        broker: "BaseBroker",
        result_backend: "IResultBackend | None" = None,
    ) -> None:
        self._connection = connection
        self._broker = broker
        self._result_backend = result_backend

    @final
    def start_consume(
        self,
        queue: str = DEFAULT_QUEUE,
        processes: int = 1,
    ) -> None:
        cron_queue = f"{CRON_QUEUE_PREFIX}{queue}"
        if processes < 1:
            raise ValueError("Param 'processes' must be positive integer (without zero)")
        if processes == 1:
            self._consumer_loop(cron_queue=cron_queue, consumer_id=0)
            return
        running_processes = []
        for process_num in range(processes):
            process = multiprocessing.Process(
                name=f"melony-cron-process-{process_num}",
                target=self._consumer_loop,
                args=(cron_queue, process_num),
                daemon=False,
            )
            running_processes.append(process)
        try:
            self._run_all_processes(running_processes)
        except KeyboardInterrupt:
            for process in running_processes:
                if process.is_alive():
                    process.terminate()

    @final
    def _consumer_loop(self, cron_queue: str, consumer_id: int = 0) -> None:
        self._initialize_cron_tasks(cron_queue)
        log_info(f"Start listening cron queue {cron_queue}", consumer_id=consumer_id)
        while True:
            try:
                self._consumer_loop_iteration(cron_queue, consumer_id)
            except ConnectionError as exc:
                log_error("Redis connection error", exc=exc)
                break
            except Exception as exc:
                log_error("Unexpected error at cron consuming loop", exc=exc)
                break
            time.sleep(self._poll_interval_secs)

    @final
    def _consumer_loop_iteration(
        self,
        cron_queue: str,
        consumer_id: int,
    ) -> None:
        pop_due = self._pop_due_entries(cron_queue)
        for entry in pop_due:
            self._execute_entry(entry, cron_queue, consumer_id)

    @final
    def _execute_entry(
        self,
        entry: CronEntry,
        cron_queue: str,
        consumer_id: int,
    ) -> None:
        func = find_task_func(entry.func_path)
        remaining = entry.retries
        attempt_num = 0
        while remaining > 0:
            remaining -= 1
            attempt_num += 1
            try:
                func()
            except Exception as exc:
                log_error(
                    _CRON_FAILED_MSG.format(entry.func_path, attempt_num),
                    exc=exc,
                    consumer_id=consumer_id,
                )
                if remaining > 0:
                    time.sleep(entry.retry_timeout)
                continue
            log_info(_CRON_COMPLETE_MSG.format(entry.func_path), consumer_id=consumer_id)
            break
        self._reschedule(entry, cron_queue)

    @final
    def _run_consumer_in_process(self, cron_queue: str, consumer_id: int = 0) -> None:
        self._consumer_loop(cron_queue=cron_queue, consumer_id=consumer_id)

    @abstractmethod
    def _pop_due_entries(self, cron_queue: str) -> list[CronEntry]:
        """Atomically pop all due cron entries from the sorted set."""

    @abstractmethod
    def _initialize_cron_tasks(self, cron_queue: str) -> None:
        """Initialize cron tasks."""

    @abstractmethod
    def _reschedule(self, entry: CronEntry, cron_queue: str) -> None:
        """Reschedule a cron entry with its next execution time."""
