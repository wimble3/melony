from redis.asyncio.client import Redis

from melony.consumers.redis_consumer import RedisConsumer
from melony.core.consumers import BaseConsumer
from melony.core.publishers import IPublisher
from melony.core.brokers import BaseBroker
from melony.core.result_backend import IResultBackend
from melony.publishers.redis_publisher import RedisPublisher
from melony.core.json_task_converter import JsonTaskConverter


class RedisBroker(BaseBroker):
    def __init__(
        self,
        connection_str: str, 
        result_backend: IResultBackend | None = None
    ) -> None:
        self._connection = Redis.from_url(connection_str)
        self._result_backend = result_backend
        
    @property
    def publisher(self) -> IPublisher:
        return RedisPublisher(connection=self._connection)

    @property
    def consumer(self) -> BaseConsumer:
        return RedisConsumer(
            publisher=self.publisher,
            broker=self,
            result_backend=self._result_backend
        )
    