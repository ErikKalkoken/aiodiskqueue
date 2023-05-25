"""Core implementation of a persistent AsyncIO queue."""

import pickle
from pathlib import Path
from typing import Any, Union

import aiosqlite

from aiodiskqueue.exceptions import QueueEmpty


class Queue:
    """A persistent AsyncIO queue.

    The queue has no upper limited and is constrained by available disk space only.

    Create a new object with the factory method :func:`create`.
    """

    def __init__(self, db_path: Union[str, Path]) -> None:
        """Note that when calling this method it is assumed
        that the queue DB already exists at the given path.

        Args:
            db_path: Path of an existing queue DB
        """
        self.db_path = Path(db_path)

    async def qsize(self) -> int:
        """Return the approximate size of the queue.
        Note, qsize() > 0 doesn’t guarantee that a subsequent get()
        will not raise :class:`.QueueEmpty`.
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
        """Put an item into the queue.

        Args:
            item: Any Python object that can be pickled
        """
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
        """Create a new queue object.

        A new queue DB will be created for this queue if it does not exist.

        If the queue DB does exist it will be reused
        and it's content will be preserved.

        Args:
            db_path: Path of the SQLite DB to be created / used. e.g. `queue.sqlite`

        Returns:
            newly created queue object
        """
        db_path = Path(db_path)
        async with aiosqlite.connect(db_path, isolation_level=None) as db:
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS queue (item BLOB);
                """
            )
        return cls(db_path)
