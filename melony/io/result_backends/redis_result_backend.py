import asyncio
import json

from typing import Sequence, final, override
from redis.asyncio import Redis

from melony.core.consts import REDIS_RESULT_BACKEND_KEY
from melony.core.dto import TaskResultDTO
from melony.core.result_backends import IAsyncResultBackend


@final
class RedisResultBackend(IAsyncResultBackend):
    def __init__(self, connection_str: str) -> None:
        self._connection = Redis.from_url(connection_str)

    @override
    async def save_results(self, task_results: Sequence[TaskResultDTO]) -> None:
        set_result_coroutines = []

        for task_result_info in task_results:
            task_id = task_result_info.task.task_id
            redis_key = f"{REDIS_RESULT_BACKEND_KEY}{task_id}"
            set_result_coroutines.append(
                self._connection.set(
                    name=redis_key,
                    value=json.dumps(task_result_info.task_result)
                )
            )
        await asyncio.gather(*set_result_coroutines)
