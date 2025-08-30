from abc import ABC, abstractmethod


class BaseConsumer(ABC):
    @abstractmethod
    async def start_consume(self):
        ...
