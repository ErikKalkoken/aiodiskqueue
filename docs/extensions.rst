.. currentmodule:: aiodiskqueue

===============
Customization
===============

Storage Engines
===============

aiodiskqueue uses the DbmEngine as default, but you can also select a different storage engine.

Or you can create your own storage engine by inheriting from :class:`.FifoStorageEngine`.

.. automodule:: aiodiskqueue.engines.dbm

.. automodule:: aiodiskqueue.engines.simple

.. automodule:: aiodiskqueue.engines.sqlite

.. automodule:: aiodiskqueue.engines.base
