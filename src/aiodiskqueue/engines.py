"""Engines for storing the queues on disk."""

import io
import logging
import pickle
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Iterable, List

import aiofiles
import aiofiles.os

logger = logging.getLogger(__name__)


class _StorageEngine(ABC):
    """Base class for all storage engines."""

    def __init__(self, data_path: Path) -> None:
        self._data_path = data_path

    @property
    @abstractmethod
    def can_append(self) -> bool:  # type: ignore
        """Can append items to data file.

        :meta private:
        """

    @abstractmethod
    async def load_all_items(self) -> List[Any]:  # type: ignore
        """Load all items from data file.

        :meta private:
        """

    @abstractmethod
    async def save_all_items(self, items: List[Any]):
        """Saves all items to data file.

        :meta private:
        """

    async def append_item(self, item: Any):
        """Append one item to data file.

        :meta private:
        """
        raise NotImplementedError()


class PickledList(_StorageEngine):
    """This engine stored items as one singular pickled list of items."""

    @property
    def can_append(self) -> bool:
        return False

    async def load_all_items(self) -> List[Any]:
        try:
            async with aiofiles.open(self._data_path, "rb", buffering=0) as file:
                data = await file.read()
        except FileNotFoundError:
            return []

        try:
            queue = pickle.loads(data)
        except pickle.PickleError:
            backup_path = self._data_path.with_suffix(".bak")
            await aiofiles.os.rename(self._data_path, backup_path)
            logger.exception(
                "Data file is corrupt and has been backed up: %s", backup_path
            )
            return []

        return queue

    async def save_all_items(self, items: Iterable[Any]):
        async with aiofiles.open(self._data_path, "wb", buffering=0) as file:
            await file.write(pickle.dumps(items))


class PickleSequence(_StorageEngine):
    """This engine stores items as a sequence of single pickles."""

    @property
    def can_append(self) -> bool:
        return True

    async def load_all_items(self) -> List[Any]:
        try:
            async with aiofiles.open(self._data_path, "rb", buffering=0) as file:
                pickled_bytes = await file.read()
        except FileNotFoundError:
            return []

        items = []
        with io.BytesIO() as buffer:
            buffer.write(pickled_bytes)
            buffer.seek(0)
            while True:
                try:
                    item = pickle.load(buffer)
                except EOFError:
                    break
                except pickle.PickleError:
                    backup_path = self._data_path.with_suffix(".bak")
                    await aiofiles.os.rename(self._data_path, backup_path)
                    logger.exception(
                        "Data file is corrupt and has been backed up: %s", backup_path
                    )
                    return []
                else:
                    items.append(item)
        return items

    async def save_all_items(self, items: Iterable[Any]):
        with io.BytesIO() as buffer:
            for item in items:
                pickle.dump(item, buffer)
            buffer.seek(0)
            data = buffer.read()
        async with aiofiles.open(self._data_path, "wb", buffering=0) as file:
            await file.write(data)

    async def append_item(self, item: Any):
        async with aiofiles.open(self._data_path, "ab", buffering=0) as file:
            await file.write(pickle.dumps(item))
