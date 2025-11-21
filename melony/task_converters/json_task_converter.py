import json

from melony.core.brokers import BaseBroker
from melony.core.task_converters import ITaskConverter
from melony.core.task_finders import find_task_func
from melony.core.tasks import Task


class JsonTaskConverter(ITaskConverter):
    def serialize_task(self, task: Task) -> str:
        return task.as_json()

    def deserialize_task(
        self,
        serialized_task: str,
        broker: BaseBroker
    ) -> Task:
        task_dict = json.loads(serialized_task)
        task_func_path = task_dict["func_path"]
        task_func = find_task_func(task_func_path)
        return Task(
            task_id=task_dict["task_id"],
            kwargs=task_dict["kwargs"],
            countdown=task_dict["countdown"],
            timestamp=task_dict["timestamp"],
            func=task_func,
            func_path=task_func_path,
            broker=broker
        )

