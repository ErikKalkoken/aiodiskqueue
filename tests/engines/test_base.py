# type: ignore


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
        await self.engine.add_item("alpha")
        # then
        items = await self.engine.fetch_all()
        self.assertListEqual(items, ["alpha"])

    async def test_can_add_item_multiple_items_to_new_db(self):
        # given
        await self.engine.initialize()
        # when
        await self.engine.add_item("alpha")
        await self.engine.add_item("bravo")
        await self.engine.add_item("charlie")
        # then
        items = await self.engine.fetch_all()
        self.assertListEqual(items, ["alpha", "bravo", "charlie"])

    async def test_roundtrip_for_single_item(self):
        # given
        await self.engine.initialize()
        # when
        await self.engine.add_item("alpha")
        await self.engine.remove_item()
        # then
        items = await self.engine.fetch_all()
        self.assertListEqual(items, [])

    async def test_roundtrip_for_multiple_item(self):
        # given
        await self.engine.initialize()
        # when
        await self.engine.add_item("alpha")
        await self.engine.add_item("bravo")
        await self.engine.remove_item()
        # then
        items = await self.engine.fetch_all()
        self.assertListEqual(items, ["bravo"])

    async def test_fetch_all_one_missing_file_returns_empty_list(self):
        # when
        items = await self.engine.fetch_all()
        # then
        self.assertListEqual(items, [])
