import datetime as dt
import shutil
import tempfile
from pathlib import Path
from unittest import IsolatedAsyncioTestCase

from aiodiskqueue import PersistentQueue, QueueEmpty


class TestPersistentQueue(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.temp_dir = Path(tempfile.mkdtemp())
        self.db_path = self.temp_dir / "test_queue.sqlite"

    def tearDown(self) -> None:
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_should_not_permit_direct_instantiation(self):
        # when/then
        with self.assertRaises(TypeError):
            PersistentQueue(self.db_path)

    async def test_should_create_queue_and_measure_size(self):
        # given
        q = await PersistentQueue.create(self.db_path)
        # when
        result = await q.qsize()
        # then
        self.assertEqual(result, 0)

    async def test_should_put_items_and_measure_size(self):
        # given
        q = await PersistentQueue.create(self.db_path)
        # when
        await q.put("dummy")
        # then
        result = await q.qsize()
        self.assertEqual(result, 1)

    async def test_should_get_items(self):
        # given
        q = await PersistentQueue.create(self.db_path)
        await q.put("dummy")
        # when
        result = await q.get()
        # then
        self.assertEqual(result, "dummy")

    async def test_should_handle_complex_items(self):
        # given
        q = await PersistentQueue.create(self.db_path)
        item = {
            "alpha": ["one", "two", "three"],
            "now": dt.datetime.now(tz=dt.timezone.utc),
        }
        await q.put(item)
        # when
        result = await q.get()
        # then
        self.assertEqual(result, item)

    async def test_should_raise_exception_when_get_on_empty_queue(self):
        # given
        q = await PersistentQueue.create(self.db_path)
        # when/then
        with self.assertRaises(QueueEmpty):
            await q.get()

    async def test_should_report_as_empty(self):
        # given
        q = await PersistentQueue.create(self.db_path)
        # when/then
        self.assertTrue(await q.empty())

    async def test_should_not_report_as_empty(self):
        # given
        q = await PersistentQueue.create(self.db_path)
        await q.put("dummy")
        # when/then
        self.assertFalse(await q.empty())
