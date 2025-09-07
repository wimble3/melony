import asyncio

from pkg_example.other_pkg.tasks import example_task, broker

# from melony.brokers.redis import RedisBroker

# broker = RedisBroker("redis://localhost:6379/0")



async def main():
    await example_task(number=2, string_param="first task").delay()
    await example_task(number=2, string_param="second task").delay()
    await example_task(number=2, string_param="second task").delay()
    await example_task(number=2, string_param="second task").delay(countdown=3)
    await example_task(number=2, string_param="second task").delay(countdown=5)
    await example_task(number=2, string_param="second task").delay(countdown=3)
    await broker.start_consume()

if __name__ == "__main__":
    asyncio.run(main())
