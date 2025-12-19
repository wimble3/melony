from abc import ABC, abstractmethod
from redis.asyncio import Redis
from redis import Redis as SyncRedis

from melony.core.tasks import Task


type Publisher = IAsyncPublisher | ISyncPublisher


class IAsyncPublisher(ABC):
    @property
    @abstractmethod
    def connection(self) -> Redis:
        ...

    @abstractmethod
    async def push(self, task: Task) -> None:
        ...

class ISyncPublisher(ABC):
    @property
    @abstractmethod
    def connection(self) -> SyncRedis:
        ...

    @abstractmethod
    def push(self, task: Task) -> None:
        ...
