import asyncio

from pkg_example.other_pkg.tasks import broker


async def main():
    await broker.consumer.start_consume()

if __name__ == "__main__":
    asyncio.run(main())
