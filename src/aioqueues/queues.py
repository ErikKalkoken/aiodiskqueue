import asyncio
import pickle
from pathlib import Path
from typing import Any, Union

import aiosqlite


class PersistentQueueException(Exception):
    """Top exception for exceptions raised by this package."""


class QueueEmpty(PersistentQueueException):
    """The queue is empty."""


class PersistentQueue:
    """A permanent, unlimited queue designed for asyncio."""

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path

    async def qsize(self) -> int:
        """Return the approximate size of the queue.
        Note, qsize() > 0 doesn’t guarantee that a subsequent get() will not block.
        """
        async with aiosqlite.connect(self.db_path, isolation_level=None) as db:
            rows: list = await db.execute_fetchall(
                """
                SELECT count(*) FROM queue;
                """
            )  # type: ignore
        return int(rows[0][0])

    async def empty(self) -> bool:
        """Return True if the queue is empty, False otherwise.

        If empty() returns False it doesn’t guarantee
        that a subsequent call to get() will not block.
        """
        return await self.qsize() == 0

    async def get(self) -> Any:
        """Remove and return an item from the queue.
        If queue is empty, raise QueueEmpty.
        """
        async with aiosqlite.connect(self.db_path, isolation_level=None) as db:
            db.row_factory = aiosqlite.Row
            rows: list = await db.execute_fetchall(
                """
                DELETE FROM queue
                RETURNING *
                ORDER BY rowid
                LIMIT 1;
                """
            )  # type: ignore
            if rows:
                return pickle.loads(rows[0]["item"])
            raise QueueEmpty()

    async def put(self, item: Any) -> None:
        """Put an item into the queue."""
        data = pickle.dumps(item)
        async with aiosqlite.connect(self.db_path, isolation_level=None) as db:
            await db.execute(
                """
                INSERT INTO queue (item) VALUES (?);
                """,
                (data,),
            )

    @classmethod
    async def create(cls, db_path: Union[str, Path]) -> "PersistentQueue":
        """Create new queue."""
        db_path = Path(db_path)
        async with aiosqlite.connect(db_path, isolation_level=None) as db:
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS queue (item BLOB);
                """
            )
        return cls(db_path)
