from dataclasses import dataclass
from typing import Any, Sequence

from melony.core.tasks import Task


@dataclass(frozen=True, kw_only=True)
class FilteredTasksDTO:
    tasks_to_execute: Sequence[Task]
    tasks_to_push_back: Sequence[Task]


@dataclass(frozen=True, kw_only=True)
class TaskResultDTO:
    task: Task
    task_result: Any


@dataclass(frozen=True, kw_only=True)
class WaitTaskResultsDTO:
    tasks_with_result: Sequence[TaskResultDTO]
    tasks_to_retry: Sequence[Task]
