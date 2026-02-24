import asyncio

from tasks import example_task, async_example_task


async def main() -> None:
    # sync delaying
    # example_task(string_param='Im sync').delay()
    
    # async delaying
    await async_example_task(integer=34).delay(countdown=30)
    
    

if __name__ == '__main__':
    asyncio.run(main())
