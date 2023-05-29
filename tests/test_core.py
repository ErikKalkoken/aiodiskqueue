import asyncio

import aiofiles
import aiofiles.os

from aiodiskqueue import Queue, QueueEmpty, QueueFull

from .factories import ItemFactory
from .helpers import QueueAsyncioTestCase


class TestCreateQueue(QueueAsyncioTestCase):
    async def test_should_create_queue_and_measure_size(self):
        # given
        q = await Queue.create(self.data_path)
        # when/then
        self.assertIsInstance(q, Queue)

    def test_should_raise_error_when_trying_to_instantiate_directly(self):
        # when/then
        with self.assertRaises(TypeError):
            Queue()

    async def test_creating_queue_with_maxsize_below_0_yields_unlimited_queue(self):
        # when
        q = await Queue.create(self.data_path, maxsize=-1)
        # then
        self.assertEqual(q.maxsize, 0)

    async def test_should_reset_queue_and_backup_data_file_when_not_usable(self):
        # given
        async with aiofiles.open(self.data_path, "wb") as fp:
            await fp.write(b"invalid-data")
        # when
        q = await Queue.create(self.data_path)
        # then
        self.assertEqual(q.qsize(), 0)
        backup_path = self.data_path.with_suffix(".bak")
        self.assertTrue(backup_path.exists())

    async def test_should_overwrite_existing_backup(self):
        # given
        async with aiofiles.open(self.data_path, "wb") as fp:
            await fp.write(b"invalid-data")
        backup_path = self.data_path.with_suffix(".bak")
        backup_path.touch()
        # when
        q = await Queue.create(self.data_path)
        # then
        self.assertEqual(q.qsize(), 0)
        self.assertTrue(backup_path.exists())

    async def test_should_preserve_queue_content(self):
        # given
        queue_1 = await Queue.create(self.data_path)
        item = ItemFactory()
        await queue_1.put_nowait(item)
        del queue_1
        # when
        queue_2 = await Queue.create(self.data_path)
        # then
        item_new = await queue_2.get()
        self.assertEqual(item_new, item)


class TestPutIntoQueue(QueueAsyncioTestCase):
    async def test_should_put_items_and_measure_size(self):
        # given
        q = await Queue.create(self.data_path)
        # when
        await q.put_nowait(ItemFactory())
        await q.put_nowait(ItemFactory())
        # then
        result = q.qsize()
        self.assertEqual(result, 2)

    async def test_should_delete_file_when_queue_is_empty(self):
        # given
        q = await Queue.create(self.data_path)
        await q.put_nowait(ItemFactory)
        # when
        await q.get()
        # then
        self.assertFalse(self.data_path.exists())

    async def test_should_raise_error_when_putting_into_full_queue(self):
        # given
        q = await Queue.create(self.data_path, maxsize=1)
        await q.put_nowait(ItemFactory)
        # when
        with self.assertRaises(QueueFull):
            await q.put_nowait(ItemFactory)

    async def test_put_should_block_until_queue_has_free_slots(self):
        async def producer(item):
            await q.put(item)

        async def consumer():
            return await q.get()

        # given
        q = await Queue.create(self.data_path, maxsize=1)
        await q.put_nowait("item-1")

        # when
        producer_task = asyncio.create_task(producer("item-2"))
        await asyncio.sleep(1)  # producer sees a full queue when task starts
        consumer_task = asyncio.create_task(consumer())
        await asyncio.gather(producer_task, consumer_task)

        item = await q.get_nowait()
        # then
        self.assertEqual(item, "item-2")


class TestRetrieveFromQueue(QueueAsyncioTestCase):
    async def test_should_get_item(self):
        # given
        q = await Queue.create(self.data_path)
        item = ItemFactory()
        await q.put_nowait(item)
        await q.put_nowait(ItemFactory())
        # when
        result = await q.get_nowait()
        # then
        self.assertEqual(result, item)

    async def test_should_raise_exception_when_get_on_empty_queue(self):
        # given
        q = await Queue.create(self.data_path)
        # when/then
        with self.assertRaises(QueueEmpty):
            await q.get_nowait()

    async def test_get_should_wait_until_item_is_available(self):
        async def consumer():
            item = await input_queue.get()
            await result_queue.put(item)

        # given
        input_queue = await Queue.create(self.data_path)
        result_queue = asyncio.Queue()
        asyncio.create_task(consumer())
        # when
        await asyncio.sleep(1)  # consumer sees an empty queue when task starts
        item = ItemFactory()
        await input_queue.put_nowait(item)
        # then
        item_new = await result_queue.get()
        self.assertEqual(item_new, item)

    async def test_should_raise_error_when_calling_task_done_too_often(self):
        # given
        q = await Queue.create(self.data_path)
        await q.put_nowait(ItemFactory)
        await q.get()
        await q.task_done()
        # when/then
        with self.assertRaises(ValueError):
            await q.task_done()


class TestWaitUntilQueueIsEmpty(QueueAsyncioTestCase):
    async def test_join_should_block_until_all_tasks_completed(self):
        async def consumer(queue: Queue):
            while True:
                await queue.get()
                await queue.task_done()

        # given
        queue = await Queue.create(self.data_path)
        for _ in range(10):
            await queue.put_nowait(ItemFactory())

        consumer_task = asyncio.create_task(consumer(queue))
        # when
        await queue.join()
        # then
        consumer_task.cancel()
        result = queue.empty()
        self.assertTrue(result)

    async def test_join_return_when_queue_is_empty(self):
        # given
        q = await Queue.create(self.data_path)
        # when/then
        await q.join()


class TestInformationAboutQueue(QueueAsyncioTestCase):
    async def test_should_report_as_empty(self):
        # given
        q = await Queue.create(self.data_path)
        # when/then
        self.assertTrue(q.empty())

    async def test_should_not_report_as_empty(self):
        # given
        q = await Queue.create(self.data_path)
        await q.put_nowait(ItemFactory())
        # when/then
        self.assertFalse(q.empty())

    async def test_full_should_return_true_when_queue_is_full(self):
        # given
        q = await Queue.create(self.data_path, maxsize=1)
        await q.put_nowait(ItemFactory)
        # when/then
        self.assertTrue(q.full())

    async def test_full_should_return_false_when_queue_is_not_full(self):
        # given
        q = await Queue.create(self.data_path, maxsize=1)
        # when/then
        self.assertFalse(q.full())

    async def test_full_should_return_false_when_queue_is_not_limited(self):
        # given
        q = await Queue.create(self.data_path)
        # when/then
        self.assertFalse(q.full())
