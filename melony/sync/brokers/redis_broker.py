from typing import final
from redis import Redis as SyncRedis

from melony.core.brokers import BaseBroker
from melony.core.consumers import BaseSyncConsumer
from melony.core.publishers import ISyncPublisher
from melony.core.result_backends import ISyncResultBackend
from melony.sync.consumers.redis_consumer import RedisConsumer
from melony.sync.publishers.redis_publisher import RedisPublisher


@final
class RedisBroker(BaseBroker):
    def __init__(
        self,
        connection_str: str, 
        result_backend: ISyncResultBackend | None = None
    ) -> None:
        self._connection = SyncRedis.from_url(connection_str)
        self._result_backend = result_backend
        
    @property
    def publisher(self) -> ISyncPublisher:
        return RedisPublisher(connection=self._connection)

    @property
    def consumer(self) -> BaseSyncConsumer:
        return RedisConsumer(
            publisher=self.publisher,
            broker=self,
            result_backend=self._result_backend
        )
    