from abc import ABC, abstractmethod
from typing import Sequence

from melony.core.dto import TaskResultDTO

__all__ = ()

type ResultBackendSaver = IAsyncResultBackendSaver | ISyncResultBackendSaver


class IAsyncResultBackendSaver(ABC):
    @abstractmethod
    async def save_results(self, task_results: Sequence[TaskResultDTO]) -> None:
        ...

class ISyncResultBackendSaver(ABC):
    @abstractmethod
    def save_results(self, task_results: Sequence[TaskResultDTO]) -> None:
        ...

class IResultBackend(ABC):
    @property
    @abstractmethod
    def saver(self) -> ResultBackendSaver:
        ...
