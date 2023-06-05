"""Base class for storage engines."""

import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, List

logger = logging.getLogger("aiodiskqueue")


class FifoStorageEngine(ABC):
    """Base class for all storage engines implementing a FIFO queue."""

    def __init__(self, data_path: Path) -> None:
        self._data_path = data_path

    @abstractmethod
    async def initialize(self) -> List[Any]:
        """Initialize data file.

        :meta private:
        """

    @abstractmethod
    async def fetch_all(self) -> List[Any]:
        """Return all items in data file.

        :meta private:
        """

    @abstractmethod
    async def add_item(self, item: Any):
        """Append one item to end of data file.

        Args:
            item: Item to be appended
            items: All items including the one to be appended

        :meta private:
        """

    @abstractmethod
    async def remove_item(self):
        """Remove item from start of data file.

        Args:
            item: Item to be removed
            items: All items not including the one to be removed

        :meta private:
        """
