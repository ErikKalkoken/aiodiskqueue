# type: ignore

from aiodiskqueue import DbmEngine, PickledList, PickleSequence

from .helpers import QueueAsyncioTestCase


class TestStorageEngine:
    """Mixin with tests for testing a storage engine."""

    async def test_can_initialize(self):
        # when
        await self.engine.initialize()
        # then
        self.assertTrue(self.data_path.exists())

    async def test_can_enqueue_item_to_new_db(self):
        # when
        await self.engine.enqueue("alpha", ["alpha"])
        # then
        items = await self.engine.fetch_all()
        self.assertListEqual(items, ["alpha"])

    async def test_can_enqueue_multiple_items_to_new_db(self):
        # when
        await self.engine.enqueue("alpha", ["alpha"])
        await self.engine.enqueue("bravo", ["alpha", "bravo"])
        await self.engine.enqueue("charlie", ["alpha", "bravo", "charlie"])
        # then
        items = await self.engine.fetch_all()
        self.assertListEqual(items, ["alpha", "bravo", "charlie"])

    async def test_roundtrip_for_single_item(self):
        # when
        await self.engine.enqueue("alpha", [])
        await self.engine.dequeue([])
        # then
        items = await self.engine.fetch_all()
        self.assertListEqual(items, [])

    async def test_roundtrip_for_multiple_item(self):
        # when
        await self.engine.enqueue("alpha", ["alpha"])
        await self.engine.enqueue("bravo", ["alpha", "bravo"])
        await self.engine.dequeue(["bravo"])
        # then
        items = await self.engine.fetch_all()
        self.assertListEqual(items, ["bravo"])

    # def tearDown(self) -> None:
    #     print()
    #     print("key|content")
    #     with dbm.open(str(self.data_path), "r") as db:
    #         keys = db.keys()
    #         for key in keys:
    #             data = db.get(key)
    #             if not data:
    #                 item = None
    #             else:
    #                 try:
    #                     item = pickle.loads(data)
    #                 except pickle.PickleError:
    #                     item = data
    #             print(f"{key}|{item}")
    #     super().tearDown()


class TestPickledList(TestStorageEngine, QueueAsyncioTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.engine = PickledList(self.data_path)


class TestPickleSequence(TestStorageEngine, QueueAsyncioTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.engine = PickleSequence(self.data_path)


class TestDbmEngine(TestStorageEngine, QueueAsyncioTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.engine = DbmEngine(self.data_path)

    async def test_raise_error_when_trying_to_dequeue_from_empty_queue(self):
        # when/then
        with self.assertRaises(ValueError):
            await self.engine.dequeue([])
