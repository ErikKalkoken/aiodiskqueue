# type: ignore

from ..helpers import QueueAsyncioTestCase
from .test_base import TestStorageEngine

try:
    from aiodiskqueue.engines import PickledList
except ImportError:
    PickledList = None

try:
    from aiodiskqueue.engines import PickleSequence
except ImportError:
    PickleSequence = None

if PickledList:

    class TestPickledList(TestStorageEngine, QueueAsyncioTestCase):
        def setUp(self) -> None:
            super().setUp()
            self.engine = PickledList(self.data_path)


if PickleSequence:

    class TestPickleSequence(TestStorageEngine, QueueAsyncioTestCase):
        def setUp(self) -> None:
            super().setUp()
            self.engine = PickleSequence(self.data_path)
