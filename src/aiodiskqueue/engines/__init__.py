"""Queue storage engines."""
# flake8: noqa

from .simple import PickledList, PickleSequence

try:
    from .dbm import DbmEngine
except ImportError:
    pass

try:
    from .sqlite import SqliteEngine
except ImportError:
    pass
