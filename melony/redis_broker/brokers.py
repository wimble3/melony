from typing import assert_never, final, override
from redis.asyncio.client import Redis
from redis import Redis as SyncRedis

from melony.core.publishers import IAsyncPublisher, ISyncPublisher, Publisher
from melony.core.brokers import BaseBroker
from melony.core.result_backends import (
    IAsyncResultBackendSaver,
    IResultBackend,
    ISyncResultBackendSaver,
)
from melony.redis_broker.consumers import (
    AsyncRedisConsumer,
    SyncRedisConsumer,
    RedisConsumer
)
from melony.redis_broker.publishers import AsyncRedisPublisher, SyncRedisPublisher


@final
class RedisBroker(BaseBroker):
    def __init__(
        self, 
        redis_connection: Redis | SyncRedis,
        result_backend: IResultBackend | None = None
    ) -> None:
        self._connection = redis_connection
        self._result_backend = result_backend

    @property
    @override
    def publisher(self) -> Publisher:
        match self._connection:
            case Redis():
                return AsyncRedisPublisher(connection=self._connection)
            case SyncRedis():
                return SyncRedisPublisher(connection=self._connection)
            case _:
                assert_never(_)

    @property
    @override
    def consumer(self) -> RedisConsumer:
        match self._connection:
            case Redis():
                assert isinstance(self.publisher, IAsyncPublisher)
                if self._result_backend:
                    assert isinstance(
                        self._result_backend.saver,
                        IAsyncResultBackendSaver
                    )
                return AsyncRedisConsumer(
                    publisher=self.publisher,
                    broker=self,
                    result_backend=self._result_backend
                )
            case SyncRedis():
                assert isinstance(self.publisher, ISyncPublisher)
                if self._result_backend:
                    assert isinstance(
                        self._result_backend.saver,
                        ISyncResultBackendSaver
                    )
                return SyncRedisConsumer(
                    publisher=self.publisher,
                    broker=self,
                    result_backend=self._result_backend
                )
            case _:
                assert_never(self._connection)
