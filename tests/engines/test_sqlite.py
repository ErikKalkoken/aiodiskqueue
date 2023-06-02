# type: ignore

from ..helpers import QueueAsyncioTestCase
from .test_base import TestStorageEngine

try:
    from aiodiskqueue.engines import SqliteEngine
except ImportError:
    SqliteEngine = None


if SqliteEngine:

    class TestSqliteEngine(TestStorageEngine, QueueAsyncioTestCase):
        def setUp(self) -> None:
            super().setUp()
            self.engine = SqliteEngine(self.data_path)
