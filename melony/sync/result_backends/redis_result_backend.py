import json

from typing import Sequence, final, override
from redis import Redis as SyncRedis

from melony.core.consts import REDIS_RESULT_BACKEND_KEY
from melony.core.dto import TaskResultDTO
from melony.core.result_backends import ISyncResultBackend


@final
class RedisResultBackend(ISyncResultBackend):
    def __init__(self, connection_str: str) -> None:
        self._connection = SyncRedis.from_url(connection_str)

    @override
    def save_results(self, task_results: Sequence[TaskResultDTO]) -> None:
        for task_result_info in task_results:
            task_id = task_result_info.task.task_id
            redis_key = f"{REDIS_RESULT_BACKEND_KEY}{task_id}"
            self._connection.set(
                name=redis_key,
                value=json.dumps(task_result_info.task_result)
            )
