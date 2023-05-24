"""A persistent asyncio queue"""

from aiodiskqueue.core import PersistentQueue
from aiodiskqueue.exceptions import QueueEmpty

__version__ = "0.1.0dev1"

__all__ = ["QueueEmpty", "PersistentQueue"]