import asyncio
import pickle
from pathlib import Path
from typing import Any

import aiosqlite


class QueueEmpty(Exception):
    pass


class PersistentQueue:
    """A permanent, unlimited queue designed for asyncio."""

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path

    async def qsize(self) -> int:
        """Return the approximate size of the queue.
        Note, qsize() > 0 doesn’t guarantee that a subsequent get() will not block.
        """
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                """
                SELECT count(*) FROM queue;
                """
            ) as cursor:
                row: list = await cursor.fetchone()  # type: ignore
        return int(row[0])

    async def empty(self) -> bool:
        """Return True if the queue is empty, False otherwise.

        If empty() returns False it doesn’t guarantee
        that a subsequent call to get() will not block.
        """
        return self.qsize() == 0

    async def get(self) -> Any:
        """Remove and return an item from the queue.
        If queue is empty, raised QueueEmpty.
        """
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            rows: list = await db.execute_fetchall(
                """
                DELETE FROM queue
                RETURNING *
                ORDER BY rowid
                LIMIT 1;
                """
            )  # type: ignore
            await db.commit()
            if rows:
                return pickle.loads(rows[0]["item"])
            raise QueueEmpty()

    async def put(self, item: Any) -> None:
        """Put an item into the queue."""
        data = pickle.dumps(item)
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT INTO queue (item) VALUES (?)
                """,
                (data,),
            )
            await db.commit()

    @classmethod
    async def create(cls, db_path: Path) -> "PersistentQueue":
        async with aiosqlite.connect(db_path) as db:
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS queue (item BLOB)
                """
            )
            await db.commit()
        return cls(db_path)


async def main():
    path = Path.cwd() / "queue.sqlite"
    q = await PersistentQueue.create(path)
    await q.put("test 1")
    await q.put("test 2")
    await q.put("test 3")
    print(await q.qsize())
    while True:
        item = await q.get()
        print(item)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
