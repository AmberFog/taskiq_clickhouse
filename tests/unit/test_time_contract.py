"""Exercise DateTime64 arithmetic at Python's wider datetime boundaries."""

from datetime import UTC, datetime, timedelta

import pytest

from taskiq_clickhouse._datetime64 import (
    DATETIME64_MIN,
    add_interval,
    add_microseconds,
)


def test_microsecond_interval_rejects_python_timedelta_overflow() -> None:
    """Translate an interval too large for Python before adding it to storage time."""
    with pytest.raises(ValueError, match="deadline exceeds the supported datetime range"):
        add_microseconds(DATETIME64_MIN, 10**30, field="deadline")


@pytest.mark.parametrize(
    ("timestamp", "interval"),
    [
        (datetime.max.replace(tzinfo=UTC), timedelta(microseconds=1)),
        (datetime.min.replace(tzinfo=UTC), -timedelta(microseconds=1)),
    ],
)
def test_datetime_addition_rejects_python_range_overflow(
    timestamp: datetime,
    interval: timedelta,
) -> None:
    """Translate both upper and lower Python datetime overflow directions."""
    with pytest.raises(ValueError, match="deadline exceeds the supported datetime range"):
        add_interval(timestamp, interval, field="deadline")
