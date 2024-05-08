"""Engines for storing the queues on disk."""

import logging
import pickle
import sqlite3
from typing import Any, List

try:
    import aiosqlite
except ImportError:
    has_aiosqlite = False
else:
    has_aiosqlite = True

from .base import FifoStorageEngine

logger = logging.getLogger("aiodiskqueue")

if has_aiosqlite:

    class SqliteEngine(FifoStorageEngine):
        """A queue storage engine using Sqlite."""

        async def initialize(self):
            async with aiosqlite.connect(self._data_path, isolation_level=None) as db:
                await db.execute(
                    """
                    CREATE TABLE IF NOT EXISTS queue (item BLOB);
                    """
                )

        async def fetch_all(self) -> List[Any]:
            items = []
            try:
                async with aiosqlite.connect(
                    self._data_path, isolation_level=None
                ) as db:
                    rows: list = await db.execute_fetchall(
                        """
                            SELECT item
                            FROM queue
                            ORDER BY rowid;
                        """
                    )  # type: ignore
                    if rows:
                        for row in rows:
                            item = pickle.loads(row[0])
                            items.append(item)
            except sqlite3.OperationalError:
                pass

            return items

        async def add_item(self, item: Any):
            data = pickle.dumps(item)
            async with aiosqlite.connect(self._data_path, isolation_level=None) as db:
                await db.execute(
                    """
                    INSERT INTO queue (item) VALUES (?);
                    """,
                    (data,),
                )

        async def remove_item(self):
            async with aiosqlite.connect(self._data_path, isolation_level=None) as db:
                await db.execute(
                    """
                        DELETE FROM queue
                        WHERE rowid IN (
                            SELECT rowid
                            FROM queue
                            ORDER BY rowid
                            LIMIT 1
                        );
                    """
                )
