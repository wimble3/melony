import asyncio

from melony.brokers.mock_broker import MockBroker


broker = MockBroker()

@broker.task
async def example_task(number: int, string_param: str) -> str:
    await asyncio.sleep(1)
    result = number * 2
    return f"Processed: {result}, {string_param}"