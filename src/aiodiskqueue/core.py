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

    The queue has no upper limit and is constrained by available disk space only.

    The content of the queue will be stored on disk in a data file.
    The file only exists while there are items in the queue.

    When a new queue object is created and the data file already exists
    it will be reused to preserve it's content if possible.
    Corrupted files will be re-created.

    This class is not thread safe.

    Using two different instances with the same data file is not recommended as it may lead to data corruption.

    Args:
        data_path: Path of the data file for this queue. e.g. `queue.dat`
    """

    def __init__(self, data_path: Union[str, Path], maxsize: int = 0) -> None:
        self._data_path = Path(data_path)
        self._maxsize = maxsize
        self._data_lock = asyncio.Lock()
        self._has_new_item = asyncio.Condition()
        self._has_free_slots = asyncio.Condition()
        self._tasks_are_finished = asyncio.Condition()
        self._peak_size = 0  # measuring peak size of the queue
        self._unfinished_tasks = 0

    @property
    def maxsize(self) -> int:
        """Number of items allowed in the queue. 0 means unlimited."""
        return self._maxsize

    async def empty(self) -> bool:
        """Return True if the queue is empty, False otherwise.

        If empty() returns False it doesn't guarantee
        that a subsequent call to get() will not raise :class:`.QueueEmpty`.
        """
        return await self.qsize() == 0

    async def full(self) -> bool:
        """Return True if there are maxsize items in the queue.

        If the queue was initialized with maxsize=0 (the default),
        then full() never returns True.
        """
        if not self._maxsize:
            return False
        return await self.qsize() >= self._maxsize

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
        async with self._data_lock:
            queue = await self._read_queue()
            if not queue:
                raise QueueEmpty()

            item = queue.pop(0)
            if queue:
                await self._write_queue(queue)
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
        async with self._data_lock:
            queue = await self._read_queue()
            if self._maxsize and len(queue) >= self._maxsize:
                raise QueueFull
            queue.append(item)
            await self._write_queue(queue)
            async with self._tasks_are_finished:
                self._unfinished_tasks += 1

        async with self._has_new_item:
            self._has_new_item.notify()

    async def qsize(self) -> int:
        """Return the approximate size of the queue.
        Note, qsize() > 0 doesn't guarantee that a subsequent get()
        will not raise :class:`.QueueEmpty`.
        """
        async with self._data_lock:
            queue = await self._read_queue()
            return len(queue)

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

    async def _read_queue(self) -> list:
        try:
            async with aiofiles.open(self._data_path, "rb", buffering=0) as fp:
                data = await fp.read()
        except FileNotFoundError:
            return []

        try:
            queue = pickle.loads(data)
        except pickle.PickleError:
            logger.exception(
                "Data file is corrupt. Will be re-created: %s", self._data_path
            )
            return []

        size = len(queue)
        logger.debug("Read queue with %d items: %s", size, self._data_path)
        self._peak_size = max(size, self._peak_size)
        return queue

    async def _write_queue(self, queue):
        async with aiofiles.open(self._data_path, "wb", buffering=0) as fp:
            await fp.write(pickle.dumps(queue))
        logger.debug("Wrote queue with %d items: %s", len(queue), self._data_path)
