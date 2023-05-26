"""Core implementation of a persistent AsyncIO queue."""

import asyncio
import pickle
from pathlib import Path
from typing import Any, Union

import aiofiles

from aiodiskqueue.exceptions import QueueEmpty


class Queue:
    """A persistent AsyncIO FIFO queue.

    The queue has no upper limited and is constrained by available disk space only.

    Create a new object with the factory method :func:`create`.

    This class is not thread safe.
    """

    def __init__(self, data_path: Union[str, Path]) -> None:
        """Note that when calling this method it is assumed
        that the queue DB already exists at the given path.

        Args:
            data_path: Path of an existing data file
        """
        self.data_path = Path(data_path)
        self.lock = asyncio.Lock()
        self.has_new_item = asyncio.Condition()

    async def qsize(self) -> int:
        """Return the approximate size of the queue.
        Note, qsize() > 0 doesn’t guarantee that a subsequent get()
        will not raise :class:`.QueueEmpty`.
        """
        async with self.lock:
            queue = await self._read_queue()
            return len(queue)

    async def empty(self) -> bool:
        """Return True if the queue is empty, False otherwise.

        If empty() returns False it doesn’t guarantee
        that a subsequent call to get() will not raise :class:`.QueueEmpty`.
        """
        return await self.qsize() == 0

    async def get(self) -> Any:
        """Remove and return an item from the queue.
        If queue is empty, wait until an item is available.
        """
        while True:
            try:
                return await self.get_nowait()
            except QueueEmpty:
                pass
            async with self.has_new_item:
                await self.has_new_item.wait()

    async def get_nowait(self) -> Any:
        """Remove and return an item if one is immediately available,
        else raise :class:`.QueueEmpty`.
        """
        async with self.lock:
            queue = await self._read_queue()
            if queue:
                item = queue.pop(0)
                await self._write_queue(queue)
                return item
            raise QueueEmpty()

    async def put(self, item: Any) -> None:
        """Put an item into the queue.

        Args:
            item: Any Python object that can be pickled
        """
        async with self.lock:
            queue = await self._read_queue()
            queue.append(item)
            await self._write_queue(queue)

        async with self.has_new_item:
            self.has_new_item.notify()

    async def _read_queue(self) -> list:
        try:
            async with aiofiles.open(self.data_path, "rb") as fp:
                data = await fp.read()
                return pickle.loads(data)
        except FileNotFoundError:
            return []

    async def _write_queue(self, queue):
        async with aiofiles.open(self.data_path, "wb") as fp:
            await fp.write(pickle.dumps(queue))

    @classmethod
    async def create(cls, data_path: Union[str, Path]) -> "Queue":
        """Create a new queue object.

        A new queue DB will be created for this queue if it does not exist.

        If the queue DB does exist it will be reused
        and it's content will be preserved.

        Args:
            data_path: Path of the SQLite DB to be created / used. e.g. `queue.sqlite`

        Returns:
            newly created queue object
        """
        data_path = Path(data_path)
        return cls(data_path)
