from melony.core.consumers import BaseConsumer


class RedisConsumer(BaseConsumer):
    async def start_consume(self):
        ...