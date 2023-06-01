"""Engines for storing the queues on disk."""

import dbm
import logging
import pickle
from typing import Any, List, Optional, Union

try:
    import aiodbm
except ImportError:
    has_aiodbm = False
else:
    has_aiodbm = True

from .base import _LifoStorageEngine

logger = logging.getLogger(__name__)

if has_aiodbm:

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
        async def _get_obj(
            db: aiodbm.DbmDatabaseAsync, key: Union[str, bytes]
        ) -> Optional[Any]:
            data = await db.get(key)
            if not data:
                return None
            return pickle.loads(data)

        @staticmethod
        async def _set_obj(
            db: aiodbm.DbmDatabaseAsync, key: Union[str, bytes], item: Any
        ):
            data = pickle.dumps(item)
            await db.set(key, data)
