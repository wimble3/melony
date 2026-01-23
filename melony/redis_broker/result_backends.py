import asyncio
import json

from typing import Sequence, final, override
from redis.asyncio import Redis
from redis import Redis as SyncRedis

from melony.core.consts import REDIS_RESULT_BACKEND_KEY
from melony.core.dto import TaskResultDTO
from melony.core.result_backends import (
    IAsyncResultBackendSaver,
    IResultBackend,
    ISyncResultBackendSaver,
    ResultBackendSaver
)


@final
class _AsyncRedisResultBackendSaver(IAsyncResultBackendSaver):
    def __init__(self, redis_connection: Redis | SyncRedis) -> None:
        self._connection = redis_connection

    @override
    async def save_results(self, task_results: Sequence[TaskResultDTO]) -> None:
        set_result_coroutines = []

        for task_result_info in task_results:
            task_id = task_result_info.task.task_id
            redis_key = f"{REDIS_RESULT_BACKEND_KEY}{task_id}"
            save_task_data = {}
            save_task_data["result"] = task_result_info.task_result
            save_task_data["task"] = task_result_info.task.as_dict()
            set_result_coroutines.append(
                self._connection.set(
                    name=redis_key,
                    value=json.dumps(save_task_data)
                )
            )
        await asyncio.gather(*set_result_coroutines)


@final
class _SyncRedisResultBackendSaver(ISyncResultBackendSaver):
    def __init__(self, redis_connection: Redis | SyncRedis) -> None:
        self._connection = redis_connection

    @override
    def save_results(self, task_results: Sequence[TaskResultDTO]) -> None:
        for task_result_info in task_results:
            task_id = task_result_info.task.task_id
            redis_key = f"{REDIS_RESULT_BACKEND_KEY}{task_id}"
            save_task_data = {}
            save_task_data["result"] = task_result_info.task_result
            save_task_data["task"] = task_result_info.task.as_dict()
            self._connection.set(
                name=redis_key,
                value=json.dumps(save_task_data)
            )

@final
class RedisResultBackend(IResultBackend):
    def __init__(self, redis_connection: Redis | SyncRedis) -> None:
        self._connection = redis_connection
    
    @property
    def saver(self) -> ResultBackendSaver:
        match self._connection:
            case Redis():
                return _AsyncRedisResultBackendSaver(redis_connection=self._connection)
            case SyncRedis():
                return _SyncRedisResultBackendSaver(redis_connection=self._connection)
            case _:
                assert_never(_)

