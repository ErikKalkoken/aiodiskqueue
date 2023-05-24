class PersistentQueueException(Exception):
    """Top exception for exceptions raised by this package."""


class QueueEmpty(PersistentQueueException):
    """The queue is empty."""
