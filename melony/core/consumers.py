from abc import ABC, abstractmethod


class IConsumer(ABC):
    @abstractmethod
    async def start_consume(self) -> None:
        ...