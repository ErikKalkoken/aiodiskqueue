"""Engines for storing the queues on disk."""

import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, List

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
