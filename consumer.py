import asyncio

from tasks import async_broker


async def main() -> None:
    await async_broker.consumer.start_consume(processes=5)



if __name__ == '__main__':
    asyncio.run(main())