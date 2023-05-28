"""Load testing the Queue.

Runs multiple producers and consumers in parallel and measures duration and throughput.

For each item one put and one get is performed = 2 queue operations.
"""

import asyncio
import logging
import random
import string
import time
from pathlib import Path

import aiodiskqueue

logging.basicConfig(level="INFO", format="%(asctime)s - %(levelname)s -  %(message)s")

logger = logging.getLogger(__name__)

ITEMS_AMOUNT = 5000
PRODUCER_AMOUNT = 100
CONSUMER_AMOUNT = 2
DISKQUEUE_MAXSIZE = 1000


def random_string(length: int) -> str:
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=length))


async def producer(
    source_queue: asyncio.Queue, disk_queue: aiodiskqueue.Queue, num: int
):
    logger.info("Starting producer %d", num)
    while True:
        try:
            item = source_queue.get_nowait()
        except asyncio.QueueEmpty:
            logger.info("Stopping producer %d", num)
            return
        else:
            await disk_queue.put(item)


async def consumer(disk_queue: aiodiskqueue.Queue, result_queue: asyncio.Queue):
    logger.info("Starting consumer")
    try:
        while True:
            item = await disk_queue.get()
            await result_queue.put(item)
            await disk_queue.task_done()
    except Exception:
        logger.exception("Consumer error")


async def main(data_path):
    # create queues
    source_queue = asyncio.Queue()
    disk_queue = await aiodiskqueue.Queue.create(data_path, maxsize=DISKQUEUE_MAXSIZE)
    result_queue = asyncio.Queue()

    # create source queue with items
    source_items = {random_string(16) for _ in range(ITEMS_AMOUNT)}
    for item in source_items:
        source_queue.put_nowait(item)

    # start producer and consumers and wait for producers to finish
    start = time.perf_counter()
    consumer_tasks = [
        asyncio.create_task(consumer(disk_queue, result_queue))
        for _ in range(CONSUMER_AMOUNT)
    ]
    producers = [
        producer(source_queue, disk_queue, num + 1) for num in range(PRODUCER_AMOUNT)
    ]
    await asyncio.gather(*producers)

    # wait for consumer to finish
    logger.info("Waiting for consumer to complete...")
    await disk_queue.join()
    end = time.perf_counter()
    for task in consumer_tasks:
        task.cancel()

    # measure duration and throughput
    duration = end - start
    throughput = ITEMS_AMOUNT * 2 / duration
    logger.info("Duration: %f seconds, queue ops per sec: %f", duration, throughput)

    # compare source items with result items
    result_items = set()
    while True:
        try:
            item = result_queue.get_nowait()
        except asyncio.QueueEmpty:
            break
        else:
            result_items.add(item)
    dif = source_items.difference(result_items)
    if not dif:
        logger.info("OK")
    else:
        logger.error("Differences found")
        logger.info("dif: %s", sorted(list(dif)))

    logger.info("Peak size of disk queue was: %d", disk_queue._peak_size)


data_path = Path(__file__).parent / "loadtest_queue.dat"
data_path.unlink(missing_ok=True)
asyncio.run(main(data_path))
