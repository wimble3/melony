from melony.core.brokers import BaseBroker
from melony.core.tasks import Task, TaskJSONSerializable
from melony.logger import log_info
from melony.task_converters.json_ser_obj_task_converter import JsonSerObjTaskConverter


class MockBroker(BaseBroker, JsonSerObjTaskConverter):
    def __init__(self) -> None:
        self._queue: list[TaskJSONSerializable] = []

    async def push(
            self,
            task: Task,
    ) -> None:
        serialized_task = self.serialize_task(task)
        self._queue.append(serialized_task)
        log_info(f"Queue (len: {len(self._queue)}): {self._queue}")

    async def pop(self) -> Task | None:
        if not len(self._queue):
            return None
        serialized_task = self._queue.pop(1)
        return self.deserialize_task(serialized_task, broker=self)
