from typing import final
from redis.asyncio.client import Redis

from melony.core.consumers import BaseAsyncConsumer
from melony.core.publishers import IAsyncPublisher
from melony.core.brokers import BaseBroker
from melony.core.result_backends import IAsyncResultBackend
from melony.io.consumers.redis_consumer import RedisConsumer
from melony.io.publishers.redis_publisher import RedisPublisher


@final
class RedisBroker(BaseBroker):
    def __init__(
        self,
        connection_str: str, 
        result_backend: IAsyncResultBackend | None = None
    ) -> None:
        self._connection = Redis.from_url(connection_str)
        self._result_backend = result_backend
        
    @property
    def publisher(self) -> IAsyncPublisher:
        return RedisPublisher(connection=self._connection)

    @property
    def consumer(self) -> BaseAsyncConsumer:
        return RedisConsumer(
            publisher=self.publisher,
            broker=self,
            result_backend=self._result_backend
        )
    