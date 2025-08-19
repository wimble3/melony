from core.brokers import BaseBroker
from core.tasks import TaskWrapper
from logger import log_info


class MockBroker(BaseBroker):
    def __init__(self) -> None:
        self._task_wrappers: list[TaskWrapper] = []

    async def push(self, task_wrapper: TaskWrapper, countdown: int = 0) -> None:
        self._task_wrappers.append(task_wrapper)
        log_info(f"tasks: {self._task_wrappers}")
