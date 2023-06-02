import asyncio
import logging
from unittest import skipIf

import aiodiskqueue

from .factories import ItemFactory
from .helpers import QueueAsyncioTestCase

logger = logging.getLogger(__name__)


async def producer(
    source_queue: asyncio.Queue, disk_queue: aiodiskqueue.Queue, num: int
):
    logger.debug("Starting producer %d", num)
    while True:
        try:
            item = source_queue.get_nowait()
        except asyncio.QueueEmpty:
            logger.debug("Stopping producer %d", num)
            return
        else:
            await disk_queue.put(item)


async def consumer(disk_queue: aiodiskqueue.Queue, result_queue: asyncio.Queue):
    logger.debug("Starting consumer")
    try:
        while True:
            item = await disk_queue.get()
            await result_queue.put(item)
            await disk_queue.task_done()
    except Exception:
        logger.exception("Consumer error")


async def run_test(data_path, cls_storage_engine=None):
    # parameters
    ITEMS_AMOUNT = 100
    PRODUCER_AMOUNT = 5
    CONSUMER_AMOUNT = 2
    DISKQUEUE_MAXSIZE = 50

    # create queues
    source_queue = asyncio.Queue()
    disk_queue = await aiodiskqueue.Queue.create(
        data_path, maxsize=DISKQUEUE_MAXSIZE, cls_storage_engine=cls_storage_engine
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
        producer(source_queue, disk_queue, num + 1) for num in range(PRODUCER_AMOUNT)
    ]
    await asyncio.gather(*producers)

    # wait for consumers to finish
    logger.debug("Waiting for consumers to complete...")
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

    return source_items, result_items


class TestIntegration(QueueAsyncioTestCase):
    async def test_with_default_storage_engine(self):
        source_items, result_items = await run_test(self.data_path)
        self.assertSetEqual(source_items, result_items)

    @skipIf(not hasattr(aiodiskqueue.engines, "PickledList"), "aiofiles not installed")
    async def test_with_dbm_engine(self):
        source_items, result_items = await run_test(
            self.data_path, aiodiskqueue.engines.PickledList
        )
        self.assertSetEqual(source_items, result_items)

    @skipIf(
        not hasattr(aiodiskqueue.engines, "PickleSequence"), "aiofiles not installed"
    )
    async def test_with_pickled_sequence(self):
        source_items, result_items = await run_test(
            self.data_path, aiodiskqueue.engines.PickleSequence
        )
        self.assertSetEqual(source_items, result_items)

    @skipIf(
        not hasattr(aiodiskqueue.engines, "SqliteEngine"), "aiosqlite not installed"
    )
    async def test_with_sqlite_engine(self):
        source_items, result_items = await run_test(
            self.data_path, aiodiskqueue.engines.SqliteEngine
        )
        self.assertSetEqual(source_items, result_items)
