from dataclasses import dataclass
from typing import Any, Iterable

from melony.core.tasks import Task


__all__ = ()


@dataclass(frozen=True, kw_only=True, slots=True)
class FilteredTasksDTO:
    tasks_to_execute: Iterable[Task]
    tasks_to_push_back: Iterable[Task]


@dataclass(frozen=True, kw_only=True, slots=True)
class TaskResultDTO:
    task: Task
    task_result: Any


@dataclass(frozen=True, kw_only=True, slots=True)
class TaskExecResultsDTO:
    tasks_with_result: Iterable[TaskResultDTO]
    tasks_to_retry: Iterable[Task]


@dataclass(frozen=True, kw_only=True, slots=True)
class QueueMetaDTO:
    queue: str
    is_consumed: bool
    
