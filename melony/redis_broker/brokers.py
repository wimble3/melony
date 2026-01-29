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


__all__ = ()


@final
class RedisBroker(BaseBroker):
    """Melony redis broker implementation.

    Needs for task registration.    
    Maybe async or sync. It depends from type of your provided connection.

    Async initialization example:

        >>> from melony import RedisBroker
        >>> from redis.asyncio import Redis

        >>> async_connection = Redis(host='localhost', port=6379, db=0)
        >>> broker = RedisBroker(redis_connection=async_connection)

    Sync initialization example:

        >>> from melony import RedisBroker
        >>> from redis import Redis as SyncRedis

        >>> sync_connection = SyncRedis(host='localhost', port=6379, db=0)
        >>> broker=RedisBroker(redis_connection=sync_connection)
    
    You are able to provide result backend as well.

    Result backend example:

        >>> from melony import RedisBroker, RedisResultBackend  # or another result backend
        >>> from redis.asyncio import Redis

        >>> connection = Redis(host='localhost', port=6379, db=0)
        >>> result_backend = RedisResultBackend()
        >>> broker = RedisBroker(redis_connection=connection, result_backend=result_backend)

    See .task() method documentation for registering your tasks.

    Also you have opportunity to call special core properties: publisher and consumer.
    """
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
        """Getter for melony publisher for redis broker.
        
        For advanced users only. Usually you have not to use it.

        But if you want, you are able to push your tasks handly by .push() method of your
        publisher.

        For doing this, you should serialize task first, you are able to use your own 
        serializer or converter which already exists in melony.core.task_converters
        """
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
        """Melony redis consumer implementation.
    
        Needs for listening messages from redis broker, executing your tasks and writing 
        result backend to selected ResultBackend (see more about result backend at 
        documentation).
        
        For start consuming your tasks you need to call .start_consume() method.
        
        Run consumer example:

            >>> @broker.task
            >>> async def example_task(string_param: str) -> str:
            >>>     asyncio.sleep(2)
            >>>     return string_param.upper()

            >>> broker.consumer.start_consume()  # Run consuming process.

        Also you are able to provide 'processes' parameter for choosing number of processes 
        (workers).

        Providing processes param example:

            >>> broker.consumer.start_consume(processes=3)

        There is no recomendation how many procceses you should use. It depends from many 
        things. So, just try to understand optimal value imperatively.
        """
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
