import asyncio

from aiodiskqueue import PersistentQueue


async def main():
    q = await PersistentQueue.create("example_queue.sqlite")
    await q.put("some item")
    item = await q.get()
    print(item)


asyncio.run(main())
