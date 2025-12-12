import asyncio

from pkg_example.other_pkg.tasks import broker


async def main():
    await broker.consumer.start_consume(processes=2)

if __name__ == "__main__":
    asyncio.run(main())
