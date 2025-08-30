from melony.core.brokers import BaseBroker
from melony.core.tasks import Task
from melony.logger import log_info
from melony.task_converters.json_task_converter import JsonTaskConverter


class MockBroker(BaseBroker, JsonTaskConverter):
    def __init__(self) -> None:
        self._queue: list[str] = []

    async def push(
            self,
            task: Task,
    ) -> None:
        converted_task = self.serialize_task(task)
        self._queue.append(converted_task)
        log_info(f"Queue (len: {len(self._queue)}): {self._queue}")
        log_info(str(self.deserialize_task(converted_task)))
