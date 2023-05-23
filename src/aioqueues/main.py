import asyncio
import random
import string
from pathlib import Path

from aioqueues.queues import PersistentQueue, QueueEmpty


def random_string(length: int) -> str:
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=length))


async def producer(queue: PersistentQueue, num: int):
    for _ in range(10):
        msg = f"producer {num}: {random_string(10)}"
        await queue.put(msg)
        await asyncio.sleep(random.random())


async def consumer(queue: PersistentQueue):
    while True:
        try:
            message = await queue.get()
        except QueueEmpty:
            await asyncio.sleep(0.1)
        else:
            print(message)


async def main():
    path = Path.cwd() / "queue.sqlite"
    queue = await PersistentQueue.create(path)
    async with asyncio.TaskGroup() as tg:
        tg.create_task(consumer(queue))
        tg.create_task(producer(queue, 1))
        tg.create_task(producer(queue, 2))
        tg.create_task(producer(queue, 3))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
