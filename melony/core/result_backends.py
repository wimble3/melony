from abc import ABC, abstractmethod
from typing import Sequence

from melony.core.dto import TaskResultDTO


class IAsyncResultBackend(ABC):
    @abstractmethod
    async def save_results(self, task_results: Sequence[TaskResultDTO]) -> None:
        ...

class ISyncResultBackend(ABC):
    @abstractmethod
    def save_results(self, task_results: Sequence[TaskResultDTO]) -> None:
        ...
