"""Core implementation of a persistent asyncio queue."""

import pickle
from pathlib import Path
from typing import Any, Union

import aiosqlite

from aiodiskqueue.exceptions import QueueEmpty
from aiodiskqueue.utils import NoPublicConstructor


class Queue(metaclass=NoPublicConstructor):
    """A persistent asyncio queue.

    The queue has no upper limited and is constrained by available disk space only.

    Create a new queue with the :func:`Queue.create` factory method.
    """

    def __init__(self, _db_path: Path) -> None:
        self.db_path = _db_path

    async def qsize(self) -> int:
        """Return the approximate size of the queue.
        Note, qsize() > 0 doesn’t guarantee that a subsequent get()
        will not raise QueueEmpty.
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
        that a subsequent call to get() will not raise :class:`.QueueEmpty`.
        """
        return await self.qsize() == 0

    async def get(self) -> Any:
        """Remove and return an item from the queue.
        If queue is empty, raise :class:`.QueueEmpty`.
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
        """Put an item into the queue. Any item that can be pickled is allowed."""
        data = pickle.dumps(item)
        async with aiosqlite.connect(self.db_path, isolation_level=None) as db:
            await db.execute(
                """
                INSERT INTO queue (item) VALUES (?);
                """,
                (data,),
            )

    @classmethod
    async def create(cls, db_path: Union[str, Path]) -> "Queue":
        """Create a new queue.

        When a queue already exists at the given path it will be reused.
        """
        db_path = Path(db_path)
        async with aiosqlite.connect(db_path, isolation_level=None) as db:
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS queue (item BLOB);
                """
            )
        return cls._create(db_path)
