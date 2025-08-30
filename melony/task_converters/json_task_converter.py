from json import loads
from melony.core.task_converters import ITaskConverter
from melony.core.tasks import Task


class JsonTaskConverter(ITaskConverter):
    def serialize(self, task: Task) -> str:
        return task.as_json()

    def deserialize(self, serialized_task: str) -> Task:
        return loads(serialized_task)