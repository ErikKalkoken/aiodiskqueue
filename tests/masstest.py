import asyncio
import logging
import random
import string
import time
from pathlib import Path

import aiodiskqueue

logging.basicConfig(level="INFO", format="%(asctime)s - %(levelname)s -  %(message)s")

logger = logging.getLogger(__name__)

ITEMS_AMOUNT = 1000


def random_string(length: int) -> str:
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=length))


async def producer(queue_1: asyncio.Queue, queue_2: aiodiskqueue.Queue):
    logger.info("Starting producer")
    while True:
        try:
            item = queue_1.get_nowait()
        except asyncio.QueueEmpty:
            logger.info("Stopping producer")
            return
        else:
            await queue_2.put(item)
            await asyncio.sleep(random.random() / 10)


async def consumer(queue_2: aiodiskqueue.Queue, queue_3: asyncio.Queue):
    logger.info("Starting consumer")
    try:
        while True:
            item = await queue_2.get()
            await queue_3.put(item)
    except Exception as ex:
        logger.info("Consumer error:", ex)


async def main(db_path):
    items = [random_string(16) for _ in range(ITEMS_AMOUNT)]
    queue_1 = asyncio.Queue()
    for item in items:
        queue_1.put_nowait(item)
    queue_2 = await aiodiskqueue.Queue.create(db_path)
    queue_3 = asyncio.Queue()
    start = time.perf_counter()
    consumer_task_1 = asyncio.create_task(consumer(queue_2, queue_3))
    consumer_task_2 = asyncio.create_task(consumer(queue_2, queue_3))
    await asyncio.gather(
        producer(queue_1, queue_2),
        producer(queue_1, queue_2),
        producer(queue_1, queue_2),
        producer(queue_1, queue_2),
        return_exceptions=True,
    )
    while not await queue_2.empty():
        await asyncio.sleep(0.5)
    duration = time.perf_counter() - start
    throughput = ITEMS_AMOUNT / duration
    logger.info("Duration: %f seconds, throughput per sec: %f", duration, throughput)
    consumer_task_1.cancel()
    consumer_task_2.cancel()
    items_2 = []
    while True:
        try:
            item = queue_3.get_nowait()
        except asyncio.QueueEmpty:
            break
        else:
            items_2.append(item)
    dif = set(items).difference(set(items_2))
    if not dif:
        logger.info("OK")
    else:
        logger.error("Differences found")
        logger.info("items: %s", sorted(items))
        logger.info("items_2: %s", sorted(items_2))
        logger.info("dif: %s", sorted(list(dif)))


db_path = Path(__file__).parent / "masstest_queue.dat"
db_path.unlink(missing_ok=True)
asyncio.run(main(db_path))
db_path.unlink()
