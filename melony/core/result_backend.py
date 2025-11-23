from abc import ABC
from typing import Any, Sequence

from melony.core.dto import TaskResultDTO


class IResultBackend(ABC):
    async def save_results(self, task_results: Sequence[TaskResultDTO]) -> None:
        ...