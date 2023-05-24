"""Custom exceptions raised by this package."""


class QueueException(Exception):
    """Top exception for exceptions raised by this package."""


class QueueEmpty(QueueException):
    """The queue is empty."""
