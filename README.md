# aiodiskqueue

A persistent queue for asyncio Python.

[![release](https://img.shields.io/pypi/v/aiodiskqueue?label=release)](https://pypi.org/project/aiodiskqueue/)
[![python](https://img.shields.io/pypi/pyversions/aiodiskqueue)](https://pypi.org/project/aiodiskqueue/)
![tests](https://github.com/ErikKalkoken/aiodiskqueue/actions/workflows/main.yml/badge.svg)
[![codecov](https://codecov.io/gh/ErikKalkoken/aiodiskqueue/branch/main/graph/badge.svg?token=V43h7hl1Te)](https://codecov.io/gh/ErikKalkoken/aiodiskqueue)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

## Description

This library provides a queue, with an asyncio interface and which persists it's content on disk.

It's main advantage over the asyncio queue in the standard library is that it's content will survive a process restart.

Aiodiskqueue uses a SQLite database with autocommit to store the queue, which should enable the queue to survive most crashes and should also make it process safe (unconfirmed).

## Usage

Here is a basic example on how to use the queue

```python
import asyncio
from aiodiskqueue import Queue

async def main():
    q = await Queue.create("example_queue.sqlite")
    await q.put("some item")
    item = await q.get()
    print(item)

asyncio.run(main())

```
