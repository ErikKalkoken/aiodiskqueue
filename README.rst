============
aiodiskqueue
============

Persistent queue for Python AsyncIO.

|release| |python| |tests| |codecov| |docs| |pre-commit| |Code style: black|

Description
-----------

This library provides a persistent FIFO queue for Python AsyncIO:

- Queue content persist a process restart
- Feature parity with Python's `asyncio.Queue <https://docs.python.org/3/library/asyncio-queue.html#queue>`_
- Similar API to Python's `asyncio.Queue <https://docs.python.org/3/library/asyncio-queue.html#queue>`_
- Sane logging
- Type hints
- Fully tested
- Supports different storage engines and can be extended with custom storage engines

Usage
-----

Here is a basic example on how to use the queue:

.. code:: python

    import asyncio
    from aiodiskqueue import Queue

    async def main():
        q = await Queue.create("example_queue.sqlite")
        await q.put("some item")
        item = await q.get()
        print(item)

    asyncio.run(main())

Please see the **examples** folder for more usage examples.

Installation
------------

You can install this library directly from PyPI with the following command:

.. code:: shell

    pip install aiodiskqueue


Logging
-------

The name of the logger for all logging by this library is: ``aiodiskqueue``.

Storage Engines
---------------

aiodiskqueue support different storage engines. The default engine is `DbmEngine`.

We measured the throughput for a typical load scenario (5 producers, 1 consumer) with each storage engine:

.. image:: https://imgpile.com/images/9luzXk.png
  :width: 800
  :alt: Measurements

* `DbmEngine`: Consistent throughput at low and high volumes and about 3 x faster then Sqlite
* `PickledList`: Very fast at low volumes, but does not scale well
* `SqliteEngine`: Consistent throughput at low and high volumes. Relatively slow.

The scripts for running the measurements and generating this chart can be found in the measurements folder.


.. |release| image:: https://img.shields.io/pypi/v/aiodiskqueue?label=release
   :target: https://pypi.org/project/aiodiskqueue/

.. |python| image:: https://img.shields.io/pypi/pyversions/aiodiskqueue
   :target: https://pypi.org/project/aiodiskqueue/

.. |tests| image:: https://github.com/ErikKalkoken/aiodiskqueue/actions/workflows/main.yml/badge.svg
   :target: https://github.com/ErikKalkoken/aiodiskqueue/actions

.. |codecov| image:: https://codecov.io/gh/ErikKalkoken/aiodiskqueue/branch/main/graph/badge.svg?token=V43h7hl1Te
   :target: https://codecov.io/gh/ErikKalkoken/aiodiskqueue

.. |docs| image:: https://readthedocs.org/projects/aiodiskqueue/badge/?version=latest
   :target: https://aiodiskqueue.readthedocs.io/en/latest/?badge=latest

.. |pre-commit| image:: https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white
   :target: https://github.com/pre-commit/pre-commit

.. |Code style: black| image:: https://img.shields.io/badge/code%20style-black-000000.svg
   :target: https://github.com/psf/black
