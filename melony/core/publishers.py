from abc import ABC, abstractmethod
from redis.asyncio import Redis

from melony.core.tasks import Task


class IPublisher(ABC):
    @property
    @abstractmethod
    def connection(self) -> Redis:
        ...

    @abstractmethod
    async def push(self, task: Task) -> None:
        ...