"""Custom exceptions raised by this package."""


class QueueException(Exception):
    """Top exception for exceptions raised by this package."""


class QueueEmpty(QueueException):
    """Queue is empty."""


class QueueFull(QueueException):
    """Queue is full."""
