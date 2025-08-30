from typing import Any
from melony.core.task_converters import ITaskConverter
from melony.core.tasks import Task, TaskJSONSerializable


class JsonSerObjTaskConverter(ITaskConverter):
    def serialize_task(self, task: Task) -> TaskJSONSerializable:
        return task.as_json_serializable_obj()

    def deserialize_task(self, serialized_task: TaskJSONSerializable) -> Task:
        pass