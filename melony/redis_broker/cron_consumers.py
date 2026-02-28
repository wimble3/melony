from datetime import datetime
from typing import TYPE_CHECKING, final, override

from croniter import croniter
from redis.asyncio.client import Redis
from redis import Redis as SyncRedis

from melony.core.cron_consumers import BaseAsyncCronConsumer, BaseSyncCronConsumer
from melony.core.cron_tasks import CronEntry

if TYPE_CHECKING:
    from melony.core.brokers import BaseBroker
    from melony.core.result_backends import IResultBackend

__all__ = ()

type RedisCronConsumer = AsyncRedisCronConsumer | SyncRedisCronConsumer

_CRON_POP_SCRIPT = """ local items = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
if #items > 0 then
    redis.call('ZREM', KEYS[1], unpack(items))
end
return items
"""


@final
class AsyncRedisCronConsumer(BaseAsyncCronConsumer):
    def __init__(
        self,
        connection: Redis,
        broker: "BaseBroker",
        result_backend: "IResultBackend | None" = None,
    ) -> None:
        super().__init__(connection=connection, broker=broker, result_backend=result_backend)

    @override
    async def _pop_due_entries(self, cron_queue: str) -> list[CronEntry]:
        now = datetime.now().timestamp()
        script = self._connection.register_script(_CRON_POP_SCRIPT)
        raw_items = await script(keys=[cron_queue], args=[str(now)])
        return [CronEntry.from_json(raw.decode("utf-8")) for raw in raw_items]

    @override
    async def _initialize_cron_tasks(self, cron_queue: str) -> None:
        now = datetime.now().timestamp()
        for reg in self._broker._cron_registry:
            if reg.queue == cron_queue:
                entry = CronEntry.from_registration(reg)
                next_run = croniter(reg.cron, now).get_next(float)
                await self._connection.zadd(  # noqa: WPS476
                    cron_queue,
                    {entry.as_json(): next_run},
                    nx=True,
                )

    @override
    async def _reschedule(self, entry: CronEntry, cron_queue: str) -> None:
        now = datetime.now().timestamp()
        next_run = croniter(entry.cron, now).get_next(float)
        await self._connection.zadd(cron_queue, {entry.as_json(): next_run})


@final
class SyncRedisCronConsumer(BaseSyncCronConsumer):
    def __init__(
        self,
        connection: SyncRedis,
        broker: "BaseBroker",
        result_backend: "IResultBackend | None" = None,
    ) -> None:
        super().__init__(connection=connection, broker=broker, result_backend=result_backend)

    @override
    def _pop_due_entries(self, cron_queue: str) -> list[CronEntry]:
        now = datetime.now().timestamp()
        script = self._connection.register_script(_CRON_POP_SCRIPT)
        raw_items = script(keys=[cron_queue], args=[str(now)])
        return [CronEntry.from_json(raw.decode("utf-8")) for raw in raw_items]

    @override
    def _initialize_cron_tasks(self, cron_queue: str) -> None:
        now = datetime.now().timestamp()
        for reg in self._broker._cron_registry:
            if reg.queue == cron_queue:
                entry = CronEntry.from_registration(reg)
                next_run = croniter(reg.cron, now).get_next(float)
                self._connection.zadd(
                    cron_queue,
                    {entry.as_json(): next_run},
                    nx=True,
                )

    @override
    def _reschedule(self, entry: CronEntry, cron_queue: str) -> None:
        now = datetime.now().timestamp()
        next_run = croniter(entry.cron, now).get_next(float)
        self._connection.zadd(cron_queue, {entry.as_json(): next_run})
