"""Example: Multiple producers and a single consumer act in parallel,
each run in their own process.
"""

import asyncio
import concurrent.futures
import random
import signal
import string
import sys
from pathlib import Path

from aioqueues import PersistentQueue, QueueEmpty


def signal_handle(_signal, frame):
    """Handler to be called when process received a termination signal."""
    sys.exit()


async def producer(queue: PersistentQueue, num: int):
    for letter in string.ascii_uppercase:
        msg = f"producer {num}: {letter}"
        await queue.put(msg)
        await asyncio.sleep(random.random() / 10)


async def consumer(queue: PersistentQueue):
    while True:
        try:
            message = await queue.get()
        except QueueEmpty:
            await asyncio.sleep(0.05)
        else:
            print(message)


async def run_consumer():
    path = Path.cwd() / "example_queue.sqlite"
    queue = await PersistentQueue.create(path)
    async with asyncio.TaskGroup() as tg:
        tg.create_task(consumer(queue))


async def run_producer(num: int):
    path = Path.cwd() / "example_queue.sqlite"
    queue = await PersistentQueue.create(path)
    async with asyncio.TaskGroup() as tg:
        tg.create_task(producer(queue, num))


def run_consumer_process():
    asyncio.run(run_consumer())


def run_producer_process(num: int):
    asyncio.run(run_producer(num))


def main():
    signal.signal(signal.SIGINT, signal_handle)
    futures = []
    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures.append(executor.submit(run_consumer_process))
        futures.append(executor.submit(run_producer_process, 1))
        futures.append(executor.submit(run_producer_process, 2))
        concurrent.futures.wait(*futures)


if __name__ == "__main__":
    main()
