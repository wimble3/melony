import asyncio

from pkg_example.other_pkg.tasks import example_task



async def main():
    await example_task(number=12, string_param="param").delay(countdown=5)
    await example_task(number=4, string_param="rlly!").delay(countdown=6)
    # await example_task(number=4, string_param="big delay!").delay(countdown=15)
    # print(f"@@@ {task_result}")
    # await example_task(number=4, string_param="asdasdasd!").delay(countdown=20)
    # await example_task(number=4, string_param="5!").delay(countdown=5)

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    loop.run_until_complete(main())
    loop.close()
