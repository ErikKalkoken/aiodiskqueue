"""Engines for storing the queues with DBM."""

import dbm
import logging
import pickle
from pathlib import Path
from typing import Any, List, Optional, Union

import aiodbm

from .base import FifoStorageEngine

logger = logging.getLogger("aiodiskqueue")


class DbmEngine(FifoStorageEngine):
    """A queue storage engine using DBM."""

    def __init__(self, data_path: Path) -> None:
        super().__init__(data_path)
        self._data_path_2 = str(data_path.absolute())

    _HEAD_ID_KEY = "head_id"
    _TAIL_ID_KEY = "tail_id"

    async def initialize(self):
        async with aiodbm.open(self._data_path_2, "c") as db:
            await db.set("dummy", "test")
            await db.delete("dummy")

    async def fetch_all(self) -> List[Any]:
        try:
            async with aiodbm.open(self._data_path_2, "r") as db:
                head_id = await self._get_obj(db, self._HEAD_ID_KEY)
                tail_id = await self._get_obj(db, self._TAIL_ID_KEY)
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

    async def add_item(self, item: Any):
        async with aiodbm.open(self._data_path_2, "w") as db:
            tail_id = await self._get_obj(db, self._TAIL_ID_KEY)
            if tail_id:
                item_id = tail_id + 1
                is_first = False
            else:
                item_id = 1
                is_first = True

            await self._set_obj(db, self._make_item_key(item_id), item)
            await self._set_obj(db, self._TAIL_ID_KEY, item_id)

            if is_first:
                await self._set_obj(db, self._HEAD_ID_KEY, item_id)

    async def remove_item(self):
        async with aiodbm.open(self._data_path_2, "w") as db:
            head_id = await self._get_obj(db, self._HEAD_ID_KEY)
            tail_id = await self._get_obj(db, self._TAIL_ID_KEY)
            if not head_id or not tail_id:
                raise ValueError("Nothing to remove from an empty database")
            item_key = self._make_item_key(head_id)
            await db.delete(item_key)

            if head_id != tail_id:
                # there are items left
                await self._set_obj(db, self._HEAD_ID_KEY, head_id + 1)
            else:
                # was last item
                await db.delete(self._HEAD_ID_KEY)
                await db.delete(self._TAIL_ID_KEY)

    @staticmethod
    def _make_item_key(item_id: int) -> str:
        return f"item-{item_id}"

    @staticmethod
    async def _get_obj(db, key: Union[str, bytes]) -> Optional[Any]:
        data = await db.get(key)
        if not data:
            return None
        return pickle.loads(data)

    @staticmethod
    async def _set_obj(db, key: Union[str, bytes], item: Any):
        data = pickle.dumps(item)
        await db.set(key, data)
