# type: ignore


from ..helpers import QueueAsyncioTestCase
from .test_base import TestStorageEngine

try:
    from aiodiskqueue.engines import DbmEngine
except ImportError:
    DbmEngine = None


if DbmEngine:

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

        async def test_can_initialize(self):
            pass  # disable test for now
