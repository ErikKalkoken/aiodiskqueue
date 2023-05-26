aiodiskqueue
============

Persistent queue for Python AsyncIO.

|release| |python| |tests| |codecov| |docs| |pre-commit| |Code style: black|

Description
-----------

This library provides a persistent queue for Python AsyncIO.

It has an API similar to the standard `asyncio.Queue <https://docs.python.org/3/library/asyncio-queue.html#queue>`_,
but items in the queue will persists between process restarts.

Usage
-----

Here is a basic example on how to use the queue:

.. code:: python

    import asyncio
    from aiodiskqueue import Queue

    async def main():
        q = Queue("example_queue.sqlite")
        await q.put("some item")
        item = await q.get()
        print(item)

    asyncio.run(main())

Please see the examples folder for more usage examples.

Installation
------------

You can install directly from PyPI with the following command:

.. code:: shell

    pip install aiodiskqueue



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
