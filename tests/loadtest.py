"""Load testing the Queue.

Runs many producers and 1 consumer in parallel and measures duration and throughput.

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
PRODUCER_AMOUNT = 50


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
            await asyncio.sleep(random.random() / 10)


async def consumer(disk_queue: aiodiskqueue.Queue, result_queue: asyncio.Queue):
    logger.info("Starting consumer")
    try:
        while True:
            item = await disk_queue.get()
            await result_queue.put(item)
            await disk_queue.task_done()
    except Exception:
        logger.exception("Consumer error")


async def main(db_path):
    # create source queue with items
    items = [random_string(16) for _ in range(ITEMS_AMOUNT)]
    source_queue = asyncio.Queue()
    for item in items:
        source_queue.put_nowait(item)

    # create disk and destination queue
    disk_queue = await aiodiskqueue.Queue.create(db_path)
    result_queue = asyncio.Queue()

    # start producer and consumers and wait for producers to finish
    start = time.perf_counter()
    consumer_task = asyncio.create_task(consumer(disk_queue, result_queue))
    producers = [
        producer(source_queue, disk_queue, num + 1) for num in range(PRODUCER_AMOUNT)
    ]
    await asyncio.gather(*producers)

    # wait for consumer to finish
    logger.info("Waiting for consumer to complete...")
    await disk_queue.join()
    consumer_task.cancel()

    # measure duration and throughput
    duration = time.perf_counter() - start
    throughput = ITEMS_AMOUNT * 2 / duration
    logger.info("Duration: %f seconds, queue ops per sec: %f", duration, throughput)

    # compare source items with result items
    items_2 = []
    while True:
        try:
            item = result_queue.get_nowait()
        except asyncio.QueueEmpty:
            break
        else:
            items_2.append(item)
    dif = set(items).difference(set(items_2))
    if not dif:
        logger.info("OK")
    else:
        logger.error("Differences found")
        logger.info("dif: %s", sorted(list(dif)))

    logger.info("Peak size of disk queue was: %d", disk_queue._peak_size)


db_path = Path(__file__).parent / "loadtest_queue.dat"
db_path.unlink(missing_ok=True)
asyncio.run(main(db_path))
