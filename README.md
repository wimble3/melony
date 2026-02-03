<h1>🍈 Melony</h1>

<p align="center">
  <em>Modern task manager for python with types, async and sync support!</em>
</p>

## Features


- [x] Automatically asynchronous or synchronous, depending on provided message broker connection
- [x] Simple for users, simple for developers
- [x] Fully checked with `we-make-python-styleguide`
- [x] Fully typed for users
- [ ] Revocable tasks (pipelines)
- [ ] Powerful UI with analytics and full task control
- [ ] 100% test coverage
- [x] Great docs
- [x] No AI
- [ ] To be continue

## Quickstart

Initialize melony broker at `tasks.py`:
```python
import time
from melony import RedisBroker
from redis import Redis

broker = RedisBroker(redis_connection=Redis(host='localhost', port=6379))

@broker.task(retries=2, retry_timeout=30)
def example_task(string_param: str) -> str:
    time.sleep(5)
    return string_param.upper()
```

Delay your task anywhere:

```python
example_task(string_param='Im string param').delay(countdown=30)
```

Run your consumer at `consumer.py`
```python
from tasks import broker

broker.consumer.start_consume(processes=2)
```


## Avaible brokers

- [x] Redis
- [ ] RabbitMQ
- [ ] Kafka
- [ ] To be continue


## Installation

Using pip:
```bash
pip install melony
```

Using uv:
```bash
uv add melony
```


## Documentation

### Brokers

First of all you need to choose your broker. At this moment, you are able to use only `RedisBroker`, but very soon they will be more. Your application can have any numbers of `melony brokers`. You don't have to choose broker by async/sync parameter or import broker from io/sync packages of this lib, just import broker and create it. Thats works for all `melony` entities AT ALL! You don't have to think about this.  So, let's initialize your selected broker:


```python
from melony import RedisBroker
from redis.asyncio import Redis
from redis import Redis as SyncRedis

# async
async_redis_connection = Redis(host='localhost', port=6379, db=0)
broker = RedisBroker(redis_connection=redis_connection)

# sync
sync_redis_connection = SyncRedis(host='localhost', port=6379, db=0)  # Other connection here
broker = RedisBroker(redis_connection=redis_connection)
```

Also you are able to provide result backend to your broker. At this moment, you are able to use only `RedisResultBackend`. Result backend save your task results (return values) to selected database.


```python

from melony import RedisBroker, RedisResultBackend
from redis.asyncio import Redis
from redis import Redis as SyncRedis

# async
async_redis_connection = Redis(host='localhost', port=6379, db=0)
result_backend = RedisResultBackend(redis_connection=async_redis_connection)
broker = RedisBroker(redis_connection=redis_connection, result_backend=result_backend)

# sync
sync_redis_connection = SyncRedis(host='localhost', port=6379, db=0)
result_backend = RedisResultBackend(redis_connection=sync_redis_connection)
broker = RedisBroker(redis_connection=redis_connection)
```

### Task declaration

After your broker initilization, you are able to register task for next delaying and execution using special decorator (broker method) `.task()`, which can recieved 2 arguments: `retries` (default=1) and `retry_timeout` (default=0). Your task function must be async if you're using io message broker client and sync if it's not

```python
# async
import asyncio

from melony import RedisBroker, RedisResultBackend
from redis.asyncio import Redis

redis_connection = Redis(host='localhost', port=6379)
broker = RedisBroker(redis_connection=redis_connection)


@broker.task(retries=2, retry_timeout=30)
async def example_task(string_param: str) -> str:
    await asyncio.sleep(2)
    return string_param.upper()
```


```python
# sync
import time

from melony import RedisBroker, RedisResultBackend
from redis import Redis

redis_connection = Redis(host='localhost', port=6379)
broker = RedisBroker(redis_connection=redis_connection)


@broker.task(retries=2, retry_timeout=30)
def example_task(string_param: str) -> str:
    time.sleep(2)
    return string_param.upper()
```

You should remember, that parameter `retry_timeout` doesn't guarantee that your task will be executed literally after seconds you selected (depends from your messages value in queue). If you need more accuracy, try to configurate your consumer instances (see consumer documentation: `processes` parameter).

### Task delaying

After your tasks declaration, you are able to delay your tasks for next execution by `countdown` time. For doing this, you need to call your task as usually, then call special `TaskWrapper` method. This method calls `.delay()`. 

```python
# async
await example_task(string_param='I am async task with 30 sec countdown').delay(countdown=30)

# sync
example_task(string_param='I am sync task with 15 sec coundown').delay(countdown=15)
```

Attention: for this moment you are able to delaying tasks only for 15 minutes maximum. Later it will be 24 hours (in dev already). Also you will have opportunity to delay your tasks more then 24 hours by postgres/rabbitmq/kafka broker for long life tasks.

### Task execution normally

If your function is decorated by `@broker.task` decorator, you still are able to execute this task usually by special method `.execute()` instead `.delay()`. This mehod has no arguments and needed for immidiatly execuiton your function as common python function.

```python
# async
await example_task(string_param='I am async task for immidiatly execuiton').execute()

# sync
example_task(string_param='I am sync task for immidiatly execuiton').execute()
```

### Consuming process

Consumers need for listening messages from message broker, executing your tasks and writing result backend to selected `ResultBackend`.
To get your consumer instance use `.consumer` property of your selected broker.
For start consuming your tasks you need to call `.start_consume()` method. This method recieved 1 argument: `processes` (by default 1). There is no recomendation how many procceses you should use. It depends from many things. So, just try to understand optimal value imperatively.

```python
# async
await broker.consumer.start_consume(processes=2)

# sync
broker.consumer.start_consume(processes=3)
```

So, if you need to listening many message brokers, you have apportunity for doing this. 


### UI

IN DEV


### Revocable tasks

IN DEV

### For developers

WRITING...
