# import asyncio

# from pkg_example.async_pkg.tasks import example_task



# async def main():
#     await example_task(number=12, string_param="param").delay(countdown=5)
#     await example_task(number=4, string_param="rlly!").delay(countdown=6)

# if __name__ == "__main__":
#     loop = asyncio.new_event_loop()
#     loop.run_until_complete(main())
#     loop.close()


from pkg_example.sync_pkg.tasks import example_task


def main():
    example_task(number=2, string_param="param").delay(countdown=5)
    example_task(number=2, string_param="param").delay(countdown=5)
    example_task(number=2, string_param="param").delay(countdown=5)
    example_task(number=2, string_param="param").delay(countdown=5)
    example_task(number=2, string_param="param").delay(countdown=5)
    example_task(number=2, string_param="param").delay(countdown=5)
    example_task(number=3, string_param="rlly").delay(countdown=5)


if __name__ == "__main__":
    main()