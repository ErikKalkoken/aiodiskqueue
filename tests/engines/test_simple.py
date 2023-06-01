# type: ignore

from aiodiskqueue.engines import PickledList, PickleSequence

from ..helpers import QueueAsyncioTestCase
from .test_base import TestStorageEngine


class TestPickledList(TestStorageEngine, QueueAsyncioTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.engine = PickledList(self.data_path)


class TestPickleSequence(TestStorageEngine, QueueAsyncioTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.engine = PickleSequence(self.data_path)
