"""Persistent queues for Python AsyncIO."""

from aiodiskqueue.core import Queue
from aiodiskqueue.exceptions import QueueEmpty

__version__ = "0.1.0a4"

__all__ = ["QueueEmpty", "Queue"]
