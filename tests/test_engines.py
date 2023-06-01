# type: ignore

from aiodiskqueue.engines import DbmEngine, PickledList, PickleSequence

from .helpers import QueueAsyncioTestCase


class TestStorageEngine:
    """Mixin with tests for testing a storage engine."""

    async def test_can_initialize(self):
        # when
        await self.engine.initialize()
        # then
        self.assertTrue(self.data_path.exists())

    async def test_can_add_item_item_to_new_db(self):
        # given
        await self.engine.initialize()
        # when
        await self.engine.add_item("alpha", ["alpha"])
        # then
        items = await self.engine.fetch_all()
        self.assertListEqual(items, ["alpha"])

    async def test_can_add_item_multiple_items_to_new_db(self):
        # given
        await self.engine.initialize()
        # when
        await self.engine.add_item("alpha", ["alpha"])
        await self.engine.add_item("bravo", ["alpha", "bravo"])
        await self.engine.add_item("charlie", ["alpha", "bravo", "charlie"])
        # then
        items = await self.engine.fetch_all()
        self.assertListEqual(items, ["alpha", "bravo", "charlie"])

    async def test_roundtrip_for_single_item(self):
        # given
        await self.engine.initialize()
        # when
        await self.engine.add_item("alpha", [])
        await self.engine.remove_item([])
        # then
        items = await self.engine.fetch_all()
        self.assertListEqual(items, [])

    async def test_roundtrip_for_multiple_item(self):
        # given
        await self.engine.initialize()
        # when
        await self.engine.add_item("alpha", ["alpha"])
        await self.engine.add_item("bravo", ["alpha", "bravo"])
        await self.engine.remove_item(["bravo"])
        # then
        items = await self.engine.fetch_all()
        self.assertListEqual(items, ["bravo"])

    async def test_fetch_all_one_missing_file_returns_empty_list(self):
        # when
        items = await self.engine.fetch_all()
        # then
        self.assertListEqual(items, [])

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

    async def test_raise_error_when_trying_to_remove_item_from_empty_queue(self):
        # given
        await self.engine.initialize()
        # when/then
        with self.assertRaises(ValueError):
            await self.engine.remove_item([])
