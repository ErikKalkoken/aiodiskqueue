"""Measure queue throughput.

Runs multiple producers and consumers in parallel and measures duration and throughput.
"""
import argparse
import asyncio
import csv
import datetime as dt
import logging
import random
import string
import tempfile
import time
from dataclasses import asdict, dataclass, fields
from pathlib import Path
from typing import List

import tomllib

import aiodiskqueue

logging.basicConfig(level="INFO", format="%(asctime)s | %(levelname)s | %(message)s")

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Measurement:
    FILENAME = "measurements.csv"

    timestamp: dt.datetime
    items: int
    producers: int
    consumers: int
    peak_size: int
    profile: str
    storage_engine: str
    throughput: float
    version: str = aiodiskqueue.__version__

    def save(self):
        path = self.path()
        file_exists = path.exists()
        data = asdict(self)
        data["timestamp"] = data["timestamp"].isoformat()
        fieldnames = data.keys()
        with path.open("a") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerow(data)

        logger.info(f"Written results to: {path}")

    @classmethod
    def field_names(cls) -> List[str]:
        return [field.name for field in fields(cls)]

    @classmethod
    def path(cls) -> Path:
        return Path(__file__).parent / cls.FILENAME


def random_string(length: int) -> str:
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=length))


async def producer(
    source_queue: asyncio.Queue, disk_queue: aiodiskqueue.Queue, num: int
):
    logger.debug("Starting producer %d", num)
    while True:
        try:
            item = source_queue.get_nowait()
        except asyncio.QueueEmpty:
            logger.debug("Stopping producer %d", num)
            return
        else:
            await disk_queue.put(item)


async def consumer(disk_queue: aiodiskqueue.Queue, result_queue: asyncio.Queue):
    logger.debug("Starting consumer")
    try:
        while True:
            item = await disk_queue.get()
            await result_queue.put(item)
            await disk_queue.task_done()
    except Exception:
        logger.exception("Consumer error")


async def runner(
    data_path: Path,
    items_count: int,
    producer_count: int,
    consumer_count: int,
    timestamp: dt.datetime,
    profile_name: str,
    cls_storage_engine,
):
    logger.info(
        f"Starting run with engine {cls_storage_engine.__name__}, "
        f"profile {profile_name} and {items_count} items."
    )

    # create queues
    source_queue = asyncio.Queue()
    data_path.unlink(missing_ok=True)
    disk_queue = await aiodiskqueue.Queue.create(
        data_path, cls_storage_engine=cls_storage_engine
    )
    result_queue = asyncio.Queue()

    # create source queue with items
    source_items = {random_string(16) for _ in range(items_count)}

    if producer_count:
        for item in source_items:
            source_queue.put_nowait(item)
    else:
        for item in source_items:
            await disk_queue.put_nowait(item)

    # starting measurement
    consumer_tasks = [
        asyncio.create_task(consumer(disk_queue, result_queue))
        for _ in range(consumer_count)
    ]
    producers = [
        producer(source_queue, disk_queue, num + 1) for num in range(producer_count)
    ]
    start = time.perf_counter()
    if producers:
        await asyncio.gather(*producers)

    # wait for consumer to finish
    if consumer_tasks:
        logger.debug("Waiting for consumer to complete...")
        await disk_queue.join()
    end = time.perf_counter()

    for task in consumer_tasks:
        task.cancel()

    # measure duration and throughput
    duration = end - start
    throughput = items_count * 2 / duration
    logger.info("Throughput for %d items: %f items / sec", items_count, throughput)
    logger.info("Peak size of disk queue was: %d", disk_queue._peak_size)

    # compare source items with result items
    result_items = set()
    while True:
        try:
            item = result_queue.get_nowait()
        except asyncio.QueueEmpty:
            break
        else:
            result_items.add(item)
    dif = source_items.difference(result_items)
    if dif:
        logger.error("Differences found")
        logger.info("dif: %s", sorted(list(dif)))
        raise RuntimeError("Run failed due to differences")

    logger.info("Data integrity confirmed")

    # write results
    data = {
        "timestamp": timestamp,
        "items": items_count,
        "producers": producer_count,
        "consumers": consumer_count,
        "peak_size": disk_queue._peak_size,
        "profile": profile_name,
        "storage_engine": type(disk_queue._storage_engine).__name__,
        "throughput": throughput,
    }
    obj = Measurement(**data)
    obj.save()


async def start(
    data_path: Path,
    config: dict,
    profile_name_override=None,
    max_items=None,
):
    timestamp = dt.datetime.now(tz=dt.timezone.utc)

    # select profile if override set
    if profile_name_override:
        profiles = [
            obj for obj in config["profiles"] if obj["name"] == profile_name_override
        ]
    else:
        profiles = config["profiles"]

    # select item count if override set
    item_counts = [count for count in config["common"]["items"] if count <= max_items]

    for cls_storage_engine in [
        aiodiskqueue.engines.PickledList,
        aiodiskqueue.engines.PickleSequence,
        aiodiskqueue.engines.DbmEngine,
        aiodiskqueue.engines.SqliteEngine,
    ]:
        for profile in profiles:
            for item_count in item_counts:
                await runner(
                    data_path,
                    item_count,
                    profile["producers"],
                    profile["consumers"],
                    timestamp,
                    profile["name"],
                    cls_storage_engine,
                )


def load_config() -> dict:
    path = Path(__file__).parent / "config.toml"
    with path.open("rb") as fp:
        return tomllib.load(fp)


def main():
    config = load_config()
    profiles = sorted([obj["name"] for obj in config["profiles"]])
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--profile", choices=profiles, help="Only run the given profile"
    )
    parser.add_argument(
        "--max-items",
        type=int,
        choices=config["common"]["items"],
        help="Max items to run with from the profile",
    )
    args = parser.parse_args()

    with tempfile.TemporaryDirectory() as temp_dir:
        data_path = Path(temp_dir) / "loadtest_queue.dat"
        asyncio.run(start(data_path, config, args.profile, args.max_items))


if __name__ == "__main__":
    main()
