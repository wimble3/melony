from melony.core.consumers import IConsumer


class MockConsumer(IConsumer):
    async def start_consume(self) -> None:
        ...