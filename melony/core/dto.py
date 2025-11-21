from dataclasses import dataclass
from typing import Sequence

from melony.core.tasks import Task


@dataclass(frozen=True, kw_only=True)
class FilteredTasksDTO:
    tasks_to_execute: Sequence[Task]
    tasks_to_push_back: Sequence[Task]