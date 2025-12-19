# import asyncio
# from pkg_example.sync_pkg.tasks import broker

# async def main():
#     await broker.consumer.start_consume(processes=3)

# if __name__ == "__main__":
#     asyncio.run(main())


from pkg_example.sync_pkg.tasks import broker


def main():
    broker.consumer.start_consume(processes=2)


if __name__ == "__main__":
    main()
    
