import shutil
import tempfile
import unittest
from pathlib import Path


class QueueAsyncioTestCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.temp_dir = Path(tempfile.mkdtemp())
        self.data_path = self.temp_dir / "queue.dat"

    def tearDown(self) -> None:
        shutil.rmtree(self.temp_dir, ignore_errors=True)
