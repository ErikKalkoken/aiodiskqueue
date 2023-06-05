"""Engines for storing the queues in flat files."""

import io
import logging
import pickle
from typing import Any, List

import aiofiles
import aiofiles.os

from .base import FifoStorageEngine

logger = logging.getLogger("aiodiskqueue")


class PickledList(FifoStorageEngine):
    """This engine stores items as one singular pickled list of items."""

    async def initialize(self):
        await self._save_all_items([])

    async def fetch_all(self) -> List[Any]:
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

    async def add_item(self, item: Any):
        items = await self.fetch_all()
        items.append(item)
        await self._save_all_items(items)

    async def remove_item(self):
        items = await self.fetch_all()
        items.pop(0)
        await self._save_all_items(items)

    async def _save_all_items(self, items: List[Any]):
        async with aiofiles.open(self._data_path, "wb", buffering=0) as file:
            await file.write(pickle.dumps(items))
        logger.debug("Wrote queue with %d items: %s", len(items), self._data_path)


class PickleSequence(FifoStorageEngine):
    """This engine stores items as a sequence of single pickles."""

    async def initialize(self):
        await self._save_all_items([])

    async def fetch_all(self) -> List[Any]:
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

    async def add_item(self, item: Any):
        async with aiofiles.open(self._data_path, "ab", buffering=0) as file:
            await file.write(pickle.dumps(item))

    async def remove_item(self):
        items = await self.fetch_all()
        items.pop(0)
        await self._save_all_items(items)

    async def _save_all_items(self, items: List[Any]):
        with io.BytesIO() as buffer:
            for item in items:
                pickle.dump(item, buffer)
            buffer.seek(0)
            data = buffer.read()
        async with aiofiles.open(self._data_path, "wb", buffering=0) as file:
            await file.write(data)
        logger.debug("Wrote queue with %d items: %s", len(items), self._data_path)
