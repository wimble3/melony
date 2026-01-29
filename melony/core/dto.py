from dataclasses import dataclass
from typing import Any, Sequence

from melony.core.tasks import Task


__all__ = ()


@dataclass(frozen=True, kw_only=True, slots=True)
class FilteredTasksDTO:
    tasks_to_execute: Sequence[Task]
    tasks_to_push_back: Sequence[Task]


@dataclass(frozen=True, kw_only=True, slots=True)
class TaskResultDTO:
    task: Task
    task_result: Any


@dataclass(frozen=True, kw_only=True, slots=True)
class TaskExecResultsDTO:
    tasks_with_result: Sequence[TaskResultDTO]
    tasks_to_retry: Sequence[Task]
