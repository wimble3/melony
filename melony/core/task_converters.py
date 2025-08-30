from abc import ABC, abstractmethod
from typing import Any

from melony.core.tasks import Task


class ITaskConverter(ABC):
    @abstractmethod
    def serialize(self, task: Task) -> Any:
        ...

    @abstractmethod
    def deserialize(self, serialized_task: Any) -> Task:
        ...