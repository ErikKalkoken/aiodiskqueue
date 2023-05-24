# aiodiskqueue

A persistent asyncio queue.

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
