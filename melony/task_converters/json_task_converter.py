from json import loads
from melony.core.task_converters import ITaskConverter
from melony.core.tasks import Task
from melony.logger import log_info


class JsonTaskConverter(ITaskConverter):
    def serialize_task(self, task: Task) -> str:
        return task.as_json()

    def deserialize_task(self, serialized_task: str) -> Task:
        task_dict = loads(serialized_task)
        log_info((task_dict["func_name"]))
        
