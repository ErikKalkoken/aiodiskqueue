"""Utility classes and functions."""

from typing import Any, Type, TypeVar

T = TypeVar("T")


class NoDirectInstantiation(type):
    """Metaclass that ensures a class can not be instantiated directly.

    If a class uses this metaclass like this:

        class SomeClass(metaclass=NoPublicConstructor):
            pass

    If you try to instantiate your class (`SomeClass()`),
    a `TypeError` will be thrown.
    """

    def __call__(cls, *args, **kwargs):
        raise TypeError(
            f"{cls.__module__}.{cls.__qualname__} can not be instantiated directly"
        )

    def _create(cls: Type[T], *args: Any, **kwargs: Any) -> T:
        return super().__call__(*args, **kwargs)  # type: ignore
