from abc import ABC, abstractmethod
from redis.asyncio import Redis
from redis import Redis as SyncRedis

from melony.core.tasks import Task


__all__ = ()


type Publisher = IAsyncPublisher | ISyncPublisher


class IAsyncPublisher(ABC):
    @property
    @abstractmethod
    def connection(self) -> Redis:
        """Aync redis connection getter."""

    @abstractmethod
    async def push(self, task: Task) -> None:
        """Async push your melony task to message broker."""

class ISyncPublisher(ABC):
    @property
    @abstractmethod
    def connection(self) -> SyncRedis:
        """Sync redis connection getter."""

    @abstractmethod
    def push(self, task: Task) -> None:
        """Sync push your melony task to message broker."""
