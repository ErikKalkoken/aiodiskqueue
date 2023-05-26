import datetime as dt
from dataclasses import dataclass

import factory
import factory.fuzzy


@dataclass
class Item:
    """An item for testing the queue."""

    message: str
    timestamp: dt.datetime
    volume: float


class ItemFactory(factory.Factory):
    class Meta:
        model = Item

    message = factory.faker.Faker("sentence")
    timestamp = factory.fuzzy.FuzzyDateTime(
        dt.datetime.now(tz=dt.timezone.utc) - dt.timedelta(days=180)
    )
    volume = factory.fuzzy.FuzzyFloat(1, 100_000)
