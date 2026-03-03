import pytest
import redis
import redis.asyncio

from unittest.mock import MagicMock

from melony.redis_broker.brokers import RedisBroker
from melony.redis_broker.consumers import AsyncRedisConsumer, SyncRedisConsumer
from melony.redis_broker.cron_consumers import AsyncRedisCronConsumer, SyncRedisCronConsumer
from melony.redis_broker.publishers import AsyncRedisPublisher, SyncRedisPublisher
from melony.redis_broker.result_backends import RedisResultBackend
from melony.core.publishers import IAsyncPublisher, ISyncPublisher


def _async_conn():
    return MagicMock(spec=redis.asyncio.Redis)


def _sync_conn():
    return MagicMock(spec=redis.Redis)


def test_broker_init_stores_connection():
    conn = _async_conn()
    broker = RedisBroker(redis_connection=conn)
    assert broker._connection is conn


def test_broker_init_stores_result_backend():
    conn = _async_conn()
    backend = MagicMock()
    broker = RedisBroker(redis_connection=conn, result_backend=backend)
    assert broker._result_backend is backend


def test_broker_init_no_result_backend_defaults_to_none():
    broker = RedisBroker(redis_connection=_async_conn())
    assert broker._result_backend is None


def test_async_connection_returns_async_publisher():
    broker = RedisBroker(redis_connection=_async_conn())
    assert isinstance(broker.publisher, AsyncRedisPublisher)


def test_sync_connection_returns_sync_publisher():
    broker = RedisBroker(redis_connection=_sync_conn())
    assert isinstance(broker.publisher, SyncRedisPublisher)


def test_async_publisher_is_async_publisher_interface():
    broker = RedisBroker(redis_connection=_async_conn())
    assert isinstance(broker.publisher, IAsyncPublisher)


def test_sync_publisher_is_sync_publisher_interface():
    broker = RedisBroker(redis_connection=_sync_conn())
    assert isinstance(broker.publisher, ISyncPublisher)


def test_async_connection_returns_async_consumer():
    broker = RedisBroker(redis_connection=_async_conn())
    assert isinstance(broker.consumer, AsyncRedisConsumer)


def test_sync_connection_returns_sync_consumer():
    broker = RedisBroker(redis_connection=_sync_conn())
    assert isinstance(broker.consumer, SyncRedisConsumer)


def test_async_connection_returns_async_cron_consumer():
    broker = RedisBroker(redis_connection=_async_conn())
    assert isinstance(broker.cron_consumer, AsyncRedisCronConsumer)


def test_sync_connection_returns_sync_cron_consumer():
    broker = RedisBroker(redis_connection=_sync_conn())
    assert isinstance(broker.cron_consumer, SyncRedisCronConsumer)


def test_async_consumer_with_result_backend_covers_saver_assertion():
    conn = _async_conn()
    backend = RedisResultBackend(redis_connection=conn)
    broker = RedisBroker(redis_connection=conn, result_backend=backend)
    consumer = broker.consumer
    assert isinstance(consumer, AsyncRedisConsumer)


def test_sync_consumer_with_result_backend_covers_saver_assertion():
    conn = _sync_conn()
    backend = RedisResultBackend(redis_connection=conn)
    broker = RedisBroker(redis_connection=conn, result_backend=backend)
    consumer = broker.consumer
    assert isinstance(consumer, SyncRedisConsumer)


class _FakeConn:
    pass


def test_publisher_with_unknown_connection_type_raises():
    broker = RedisBroker(redis_connection=_FakeConn())
    with pytest.raises(Exception):
        _ = broker.publisher


def test_consumer_with_unknown_connection_type_raises():
    broker = RedisBroker(redis_connection=_FakeConn())
    with pytest.raises(Exception):
        _ = broker.consumer


def test_cron_consumer_with_unknown_connection_type_raises():
    broker = RedisBroker(redis_connection=_FakeConn())
    with pytest.raises(Exception):
        _ = broker.cron_consumer
