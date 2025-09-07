import asyncio
from pkg_example.other_pkg.tasks import example_task





async def main():
    await example_task(number=8, string_param="added task").delay(countdown=5)

if __name__ == "__main__":
    asyncio.run(main())

