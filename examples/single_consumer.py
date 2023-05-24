"""Example: Multiple producers and a single consumer act in parallel."""

import asyncio
import random
import string
from pathlib import Path

from aiodiskqueue import Queue, QueueEmpty


async def producer(queue: Queue, num: int):
    for letter in string.ascii_uppercase:
        msg = f"producer {num}: {letter}"
        await queue.put(msg)
        await asyncio.sleep(random.random() / 10)


async def consumer(queue: Queue):
    while True:
        try:
            message = await queue.get()
        except QueueEmpty:
            await asyncio.sleep(0.05)
        else:
            print(message)


async def main():
    path = Path.cwd() / "example_queue.sqlite"
    queue = Queue(path)
    coroutines = [
        consumer(queue),
        producer(queue, 1),
        producer(queue, 2),
        producer(queue, 3),
    ]
    await asyncio.gather(*coroutines)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
