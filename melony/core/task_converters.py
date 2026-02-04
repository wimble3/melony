import json

from abc import ABC, abstractmethod

from melony.core.brokers import BaseBroker
from melony.core.task_finders import find_task_func
from melony.core.tasks import _TaskMeta, AsyncTask, SyncTask, Task


__all__ = ("TaskConverter",)


class _BaseTaskConverter(ABC):
    @abstractmethod
    def deserialize_task(
        self,
        serialized_task: str,
        broker: BaseBroker
    ) -> Task:
        ...

    def serialize_task(self, task: Task) -> str:
        return task.as_json()


class TaskConverter(_BaseTaskConverter):
    def deserialize_task(
        self,
        serialized_task: str,
        broker: BaseBroker
    ) -> Task:
        task_dict = json.loads(serialized_task)
        task_func_path = task_dict["func_path"]
        task_func = find_task_func(task_func_path)

        if task_dict["is_coro"]:
            return AsyncTask(
                task_id=task_dict["task_id"],
                kwargs=task_dict["kwargs"],
                countdown=task_dict["countdown"],
                func=task_func,
                func_path=task_func_path,
                broker=broker,
                retries=task_dict["retries"],
                retry_timeout=task_dict["retry_timeout"],
                queue=task_dict["queue"],
                is_coro=task_dict["is_coro"],
                _meta=_TaskMeta(**task_dict["_meta"])
            )
        else:
            return SyncTask(
                task_id=task_dict["task_id"],
                kwargs=task_dict["kwargs"],
                countdown=task_dict["countdown"],
                func=task_func,
                func_path=task_func_path,
                broker=broker,
                retries=task_dict["retries"],
                retry_timeout=task_dict["retry_timeout"],
                queue=task_dict["queue"],
                is_coro=task_dict["is_coro"],
                _meta=_TaskMeta(**task_dict["_meta"])
            )