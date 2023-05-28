import asyncio
import logging

import aiodiskqueue

from .factories import ItemFactory
from .helpers import QueueAsyncioTestCase

logger = logging.getLogger(__name__)


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


class TestIntegration(QueueAsyncioTestCase):
    async def test_multiple_consumer_and_producers(self):
        # parameters
        ITEMS_AMOUNT = 500
        PRODUCER_AMOUNT = 10
        CONSUMER_AMOUNT = 2
        DISKQUEUE_MAXSIZE = 100

        # create queues
        source_queue = asyncio.Queue()
        disk_queue = await aiodiskqueue.Queue.create(
            self.data_path, maxsize=DISKQUEUE_MAXSIZE
        )
        result_queue = asyncio.Queue()

        # Generate items and put into source queue
        source_items = {ItemFactory() for _ in range(ITEMS_AMOUNT)}
        for item in source_items:
            source_queue.put_nowait(item)

        # start producers and consumers and wait for producers to finish
        consumer_tasks = [
            asyncio.create_task(consumer(disk_queue, result_queue))
            for _ in range(CONSUMER_AMOUNT)
        ]
        producers = [
            producer(source_queue, disk_queue, num + 1)
            for num in range(PRODUCER_AMOUNT)
        ]
        await asyncio.gather(*producers)

        # wait for consumers to finish
        logger.info("Waiting for consumers to complete...")
        await disk_queue.join()
        for task in consumer_tasks:
            task.cancel()

        # Extract result items
        result_items = set()
        while True:
            try:
                item = result_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            else:
                result_items.add(item)

        # compare result with source
        self.assertSetEqual(source_items, result_items)
