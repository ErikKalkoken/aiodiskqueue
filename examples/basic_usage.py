import asyncio

from aiodiskqueue import Queue


async def main():
    q = await Queue.create("example_queue.sqlite")
    await q.put("some item")
    item = await q.get()
    print(item)


asyncio.run(main())
