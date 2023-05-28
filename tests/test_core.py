import asyncio
import shutil
import tempfile
import tracemalloc
import unittest
from pathlib import Path

import aiofiles

from aiodiskqueue import Queue, QueueEmpty

from .factories import ItemFactory

tracemalloc.start()


class TestQueue(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.temp_dir = Path(tempfile.mkdtemp())
        self.db_path = self.temp_dir / "queue.dat"

    def tearDown(self) -> None:
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    async def test_should_create_queue_and_measure_size(self):
        # given
        q = Queue(self.db_path)
        # when
        result = await q.qsize()
        # then
        self.assertEqual(result, 0)

    async def test_should_put_items_and_measure_size(self):
        # given
        q = Queue(self.db_path)
        # when
        await q.put(ItemFactory())
        await q.put(ItemFactory())
        # then
        result = await q.qsize()
        self.assertEqual(result, 2)

    async def test_should_get_item(self):
        # given
        q = Queue(self.db_path)
        item = ItemFactory()
        await q.put(item)
        await q.put(ItemFactory())
        # when
        result = await q.get_nowait()
        # then
        self.assertEqual(result, item)

    async def test_should_raise_exception_when_get_on_empty_queue(self):
        # given
        q = Queue(self.db_path)
        # when/then
        with self.assertRaises(QueueEmpty):
            await q.get_nowait()

    async def test_should_report_as_empty(self):
        # given
        q = Queue(self.db_path)
        # when/then
        self.assertTrue(await q.empty())

    async def test_should_not_report_as_empty(self):
        # given
        q = Queue(self.db_path)
        await q.put(ItemFactory())
        # when/then
        self.assertFalse(await q.empty())

    async def test_get_should_wait_until_item_is_available(self):
        async def consumer():
            item = await q.get()
            await queue_2.put(item)

        # given
        queue_2 = asyncio.Queue()
        q = Queue(self.db_path)
        asyncio.create_task(consumer())
        # when
        await asyncio.sleep(1)  # consumer sees an empty queue when task starts
        item = ItemFactory()
        await q.put(item)
        # then
        item_new = await queue_2.get()
        self.assertEqual(item_new, item)

    async def test_should_create_new_file_when_current_file_is_corrupt(self):
        # given
        async with aiofiles.open(self.db_path, "wb") as fp:
            await fp.write(b"invalid-data")
        q = Queue(self.db_path)
        # when
        result = await q.qsize()
        # then
        self.assertEqual(result, 0)

    async def test_should_preserve_queue_content(self):
        # given
        queue_1 = Queue(self.db_path)
        item = ItemFactory()
        await queue_1.put(item)
        # when
        del queue_1
        queue_2 = Queue(self.db_path)
        item_new = await queue_2.get()
        # then
        self.assertEqual(item_new, item)

    async def test_join_should_block_until_all_tasks_completed(self):
        async def consumer(queue: Queue):
            while True:
                await queue.get()
                await queue.task_done()

        # given
        queue = Queue(self.db_path)
        for _ in range(10):
            await queue.put(ItemFactory())

        consumer_task = asyncio.create_task(consumer(queue))
        # when
        await queue.join()
        # then
        consumer_task.cancel()
        result = await queue.empty()
        self.assertTrue(result)
