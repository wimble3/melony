import json

from dataclasses import asdict, dataclass
from typing import Callable

__all__ = ()


@dataclass(frozen=True, kw_only=True, slots=True)
class CronTaskRegistration:
    func_path: str
    func: Callable
    cron: str
    queue: str
    retries: int
    retry_timeout: int
    is_coro: bool


@dataclass(frozen=True, kw_only=True, slots=True)
class CronEntry:
    func_path: str
    cron: str
    queue: str
    retries: int
    retry_timeout: int
    is_coro: bool

    def as_json(self) -> str:
        return json.dumps(asdict(self))

    @classmethod
    def from_registration(cls, reg: CronTaskRegistration) -> "CronEntry":
        return cls(
            func_path=reg.func_path,
            cron=reg.cron,
            queue=reg.queue,
            retries=reg.retries,
            retry_timeout=reg.retry_timeout,
            is_coro=reg.is_coro,
        )

    @classmethod
    def from_json(cls, serialized: str) -> "CronEntry":
        return cls(**json.loads(serialized))
