"""Core implementation of a persistent AsyncIO queue."""
import asyncio
import logging
import pickle
from pathlib import Path
from typing import Any, Union

import aiofiles

from aiodiskqueue.exceptions import QueueEmpty

logger = logging.getLogger(__name__)


class Queue:
    """A persistent AsyncIO FIFO queue.

    The queue has no upper limit and is constrained by available disk space only.

    A new data file will be created for every new queue object if it does not exist.
    If a data file already exists it will be reused
    and it's content will be preserved if possible. Corrupted files will be re-created.

    This class is not thread safe.

    Using two different instances with the same data file is not recommended as it may lead to data corruption.

    Args:
        data_path: Path of the data file for this queue. e.g. `queue.dat`
    """

    def __init__(self, data_path: Union[str, Path]) -> None:
        self._data_path = Path(data_path)
        self._lock = asyncio.Lock()
        self._has_new_item = asyncio.Condition()
        self._tasks_are_finished = asyncio.Condition()
        self._peak_size = 0  # measuring peak size of the queue
        self._unfinished_tasks = 0

    async def qsize(self) -> int:
        """Return the approximate size of the queue.
        Note, qsize() > 0 doesn’t guarantee that a subsequent get()
        will not raise :class:`.QueueEmpty`.
        """
        async with self._lock:
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
            async with self._has_new_item:
                await self._has_new_item.wait()

    async def get_nowait(self) -> Any:
        """Remove and return an item if one is immediately available,
        else raise :class:`.QueueEmpty`.
        """
        async with self._lock:
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
        async with self._lock:
            queue = await self._read_queue()
            queue.append(item)
            await self._write_queue(queue)
            async with self._tasks_are_finished:
                self._unfinished_tasks += 1

        async with self._has_new_item:
            self._has_new_item.notify()

    async def _read_queue(self) -> list:
        try:
            async with aiofiles.open(self._data_path, "rb") as fp:
                data = await fp.read()
                queue = pickle.loads(data)
                size = len(queue)
                logger.debug("Read queue with %d items: %s", size, self._data_path)
                self._peak_size = max(size, self._peak_size)
                return queue
        except FileNotFoundError:
            return []
        except pickle.PickleError:
            logger.exception(
                "Data file is corrupt. Will be re-created: %s", self._data_path
            )
            return []

    async def _write_queue(self, queue):
        async with aiofiles.open(self._data_path, "wb") as fp:
            await fp.write(pickle.dumps(queue))
        logger.debug("Wrote queue with %d items: %s", len(queue), self._data_path)

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
