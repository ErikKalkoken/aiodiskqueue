"""Engines for storing the queues on disk."""

import dbm
import io
import logging
import pickle
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, List, Optional, Union

import aiodbm
import aiofiles
import aiofiles.os
from aiodbm import DbmDatabaseAsync

logger = logging.getLogger(__name__)


class _LifoStorageEngine(ABC):
    """Base class for all storage engines implementing a LIFO queue."""

    def __init__(self, data_path: Path) -> None:
        self._data_path = data_path

    @abstractmethod
    async def initialize(self) -> List[Any]:
        """Initialize data file."""

    @abstractmethod
    async def fetch_all(self) -> List[Any]:
        """Return all items in data file."""

    @abstractmethod
    async def add_item(self, item: Any, items: List[Any]):
        """Append one item to end of data file.

        Args:
            item: Item to be appended
            items: All items including the one to be appended
        """

    @abstractmethod
    async def remove_item(self, items: List[Any]):
        """Remove item from start of data file.

        Args:
            item: Item to be removed
            items: All items not including the one to be removed
        """


class PickledList(_LifoStorageEngine):
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

    async def add_item(self, item: Any, items: List[Any]):
        await self._save_all_items(items)

    async def remove_item(self, items: List[Any]):
        await self._save_all_items(items)

    async def _save_all_items(self, items: List[Any]):
        async with aiofiles.open(self._data_path, "wb", buffering=0) as file:
            await file.write(pickle.dumps(items))
        logger.debug("Wrote queue with %d items: %s", len(items), self._data_path)


class PickleSequence(_LifoStorageEngine):
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

    async def add_item(self, item: Any, items: List[Any]):
        async with aiofiles.open(self._data_path, "ab", buffering=0) as file:
            await file.write(pickle.dumps(item))

    async def remove_item(self, items: List[Any]):
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


class DbmEngine(_LifoStorageEngine):
    """A queue storage engine using DBM."""

    HEAD_ID_KEY = "head_id"
    TAIL_ID_KEY = "tail_id"

    async def initialize(self):
        async with aiodbm.open(self._data_path, "c"):
            pass

    async def fetch_all(self) -> List[Any]:
        try:
            async with aiodbm.open(self._data_path, "r") as db:
                head_id = await self._get_obj(db, self.HEAD_ID_KEY)
                tail_id = await self._get_obj(db, self.TAIL_ID_KEY)
                if not head_id or not tail_id:
                    return []

                items = []
                for item_id in range(head_id, tail_id + 1):
                    item_key = self._make_item_key(item_id)
                    item = await self._get_obj(db, item_key)
                    items.append(item)
        except dbm.error:
            items = []

        return items

    async def add_item(self, item: Any, items: List[Any]):
        async with aiodbm.open(self._data_path, "wf") as db:
            tail_id = await self._get_obj(db, self.TAIL_ID_KEY)
            if tail_id:
                item_id = tail_id + 1
                is_first = False
            else:
                item_id = 1
                is_first = True

            await self._set_obj(db, self._make_item_key(item_id), item)
            await self._set_obj(db, self.TAIL_ID_KEY, item_id)

            if is_first:
                await self._set_obj(db, self.HEAD_ID_KEY, item_id)

            await db.sync()  # type: ignore

    async def remove_item(self, items: List[Any]):
        async with aiodbm.open(self._data_path, "wf") as db:
            head_id = await self._get_obj(db, self.HEAD_ID_KEY)
            tail_id = await self._get_obj(db, self.TAIL_ID_KEY)
            if not head_id or not tail_id:
                raise ValueError("Nothing to remove from an empty database")
            item_key = self._make_item_key(head_id)
            await db.delete(item_key)

            if head_id != tail_id:
                # there are items left
                await self._set_obj(db, self.HEAD_ID_KEY, head_id + 1)
            else:
                # was last item
                await db.delete(self.HEAD_ID_KEY)
                await db.delete(self.TAIL_ID_KEY)

            await db.sync()  # type: ignore

    @staticmethod
    def _make_item_key(item_id: int) -> str:
        return f"item-{item_id}"

    @staticmethod
    async def _get_obj(db: DbmDatabaseAsync, key: Union[str, bytes]) -> Optional[Any]:
        data = await db.get(key)
        if not data:
            return None
        return pickle.loads(data)

    @staticmethod
    async def _set_obj(db: DbmDatabaseAsync, key: Union[str, bytes], item: Any):
        data = pickle.dumps(item)
        await db.set(key, data)
