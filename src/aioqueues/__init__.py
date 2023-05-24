"""A persistent asyncio queue"""

from aioqueues.core import PersistentQueue
from aioqueues.exceptions import QueueEmpty

__version__ = "0.1.0dev1"

__all__ = ["QueueEmpty", "PersistentQueue"]
