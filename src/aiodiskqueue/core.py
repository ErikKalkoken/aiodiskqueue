"""Core implementation of a persistent AsyncIO queue."""
import asyncio
import logging
import pickle
from pathlib import Path
from typing import Any, Union

import aiofiles
import aiofiles.os

from aiodiskqueue.exceptions import QueueEmpty, QueueFull

logger = logging.getLogger(__name__)


class Queue:
    """A persistent AsyncIO FIFO queue.

    The content of a queue is stored on disk in a data file.
    This file usually only exists temporarily while there are items in the queue.

    An existing data file is used to recreate a queue,
    e.g. to preserve the queue content after a process restart

    However, should the data file be corrupted it will be discarded
    so the queue can continue to function normally.

    Using two different instances with the same data file simultaneously
    is not recommended as it may lead to data corruption.

    This class is not thread safe.

    Create a queue with the factory method :func:`create`.
    """

    def __init__(self, data_path: Path, maxsize: int, queue: list) -> None:
        """:meta private:"""
        self._data_path = Path(data_path)
        self._maxsize = max(0, maxsize)
        self._queue_lock = asyncio.Lock()
        self._has_new_item = asyncio.Condition()
        self._has_free_slots = asyncio.Condition()
        self._tasks_are_finished = asyncio.Condition()
        self._unfinished_tasks = 0
        self._peak_size = len(queue)  # measuring peak size of the queue
        self._queue = list(queue)

    @property
    def maxsize(self) -> int:
        """Number of items allowed in the queue. 0 means unlimited."""
        return self._maxsize

    def empty(self) -> bool:
        """Return True if the queue is empty, False otherwise.

        If empty() returns False it doesn't guarantee
        that a subsequent call to get() will not raise :class:`.QueueEmpty`.
        """
        return self.qsize() == 0

    def full(self) -> bool:
        """Return True if there are maxsize items in the queue.

        If the queue was initialized with maxsize=0 (the default),
        then full() always returns False.
        """
        if not self._maxsize:
            return False
        return self.qsize() >= self._maxsize

    async def get(self) -> Any:
        """Remove and return an item from the queue. If queue is empty,
        wait until an item is available.
        """
        while True:
            try:
                return await self.get_nowait()
            except QueueEmpty:
                pass
            async with self._has_new_item:
                await self._has_new_item.wait()

    async def get_nowait(self) -> Any:
        """Remove and return an item if one is immediately available,
        else raise :class:`.QueueEmpty`.
        """
        async with self._queue_lock:
            if not self._queue:
                raise QueueEmpty()

            item = self._queue.pop(0)
            if self._queue:
                await self._write_queue()
            else:
                await aiofiles.os.remove(self._data_path)

        if self._maxsize:
            async with self._has_free_slots:
                self._has_free_slots.notify()

        return item

    async def join(self) -> None:
        """Block until all items in the queue have been gotten and processed.

        The count of unfinished tasks goes up whenever an item is added to the
        queue. The count goes down whenever a consumer calls task_done() to
        indicate that the item was retrieved and all work on it is complete.
        When the count of unfinished tasks drops to zero, join() unblocks.
        """
        async with self._tasks_are_finished:
            if self._unfinished_tasks > 0:
                await self._tasks_are_finished.wait()

    async def put(self, item: Any) -> None:
        """Put an item into the queue. If the queue is full,
        wait until a free slot is available before adding the item.
        """
        while True:
            try:
                return await self.put_nowait(item)
            except QueueFull:
                pass

            async with self._has_free_slots:
                await self._has_free_slots.wait()

    async def put_nowait(self, item: Any) -> None:
        """Put an item into the queue without blocking.

        If no free slot is immediately available, raise :class:`.QueueFull`.

        Args:
            item: Any Python object that can be pickled
        """
        async with self._queue_lock:
            if self._maxsize and self.qsize() >= self._maxsize:
                raise QueueFull
            self._queue.append(item)
            await self._write_queue()
            async with self._tasks_are_finished:
                self._unfinished_tasks += 1

        async with self._has_new_item:
            self._has_new_item.notify()

    def qsize(self) -> int:
        """Return the approximate size of the queue.
        Note, qsize() > 0 doesn't guarantee that a subsequent get()
        will not raise :class:`.QueueEmpty`.
        """
        return len(self._queue)

    async def task_done(self) -> None:
        """Indicate that a formerly enqueued task is complete.

        Used by queue consumers. For each get() used to fetch a task,
        a subsequent call to task_done() tells the queue that the processing
        on the task is complete.

        If a join() is currently blocking, it will resume when all items have
        been processed (meaning that a task_done() call was received for every
        item that had been put() into the queue).

        Raises ValueError if called more times than there were items placed in
        the queue.
        """
        async with self._tasks_are_finished:
            if self._unfinished_tasks <= 0:
                raise ValueError("task_done() called too many times")

            self._unfinished_tasks -= 1
            if self._unfinished_tasks == 0:
                self._tasks_are_finished.notify_all()

    async def _write_queue(self):
        async with aiofiles.open(self._data_path, "wb", buffering=0) as fp:
            await fp.write(pickle.dumps(self._queue))
        size = self.qsize()
        self._peak_size = max(self._peak_size, size)
        logger.debug("Wrote queue with %d items: %s", size, self._data_path)

    @staticmethod
    async def _read_queue(data_path: Path) -> list:
        try:
            async with aiofiles.open(data_path, "rb", buffering=0) as fp:
                data = await fp.read()
        except FileNotFoundError:
            return []

        try:
            queue = pickle.loads(data)
        except pickle.PickleError:
            logger.exception("Data file is corrupt. Will be discarded: %s", data_path)
            return []

        if queue:
            logger.info(
                "Resurrecting queue with %d items from file: %s",
                len(queue),
                data_path,
            )
        return queue

    @classmethod
    async def create(cls, data_path: Union[str, Path], maxsize: int = 0) -> "Queue":
        """Create a queue.

        Args:
            data_path: Path of the data file for this queue. e.g. `queue.dat`
            maxsize: If maxsize is less than or equal to zero, the queue size is infinite.
                If it is an integer greater than 0, then put() blocks
                when the queue reaches maxsize until an item is removed by get().
        """
        data_path = Path(data_path)
        queue = await cls._read_queue(data_path)
        return cls(data_path, maxsize, queue)
