"""Shared DateTime64 range and arithmetic for persisted domain values."""

from datetime import datetime, timedelta
from typing import Final


DATETIME64_MIN: Final = datetime.fromisoformat("1900-01-01T00:00:00+00:00")
DATETIME64_MAX: Final = datetime.fromisoformat("2299-12-31T23:59:59.999999+00:00")

_MICROSECONDS_PER_SECOND: Final = 1_000_000
_SECONDS_PER_DAY: Final = 86_400
_DATETIME64_SPAN: Final = DATETIME64_MAX - DATETIME64_MIN


def _interval_microseconds(interval: timedelta) -> int:
    elapsed_seconds = interval.days * _SECONDS_PER_DAY + interval.seconds
    elapsed_microseconds = elapsed_seconds * _MICROSECONDS_PER_SECOND
    return elapsed_microseconds + interval.microseconds


MAX_RETENTION_INTERVAL_US: Final = _interval_microseconds(_DATETIME64_SPAN)


def require_datetime64(candidate: object, *, field: str) -> datetime:
    """Require one timezone-aware UTC value in the persisted timestamp range."""
    if not isinstance(candidate, datetime):
        msg = f"{field} must be a datetime"
        raise TypeError(msg)
    if not _is_utc(candidate):
        msg = f"{field} must be timezone-aware UTC"
        raise ValueError(msg)
    if not DATETIME64_MIN <= candidate <= DATETIME64_MAX:
        msg = f"{field} must fit DateTime64(6, 'UTC')"
        raise ValueError(msg)
    return candidate


def add_microseconds(timestamp: datetime, microseconds: int, *, field: str) -> datetime:
    """Add one exact interval and validate the resulting persisted timestamp."""
    try:
        interval = timedelta(microseconds=microseconds)
    except OverflowError as error:
        msg = f"{field} exceeds the supported datetime range"
        raise ValueError(msg) from error
    return add_interval(timestamp, interval, field=field)


def add_interval(timestamp: datetime, interval: timedelta, *, field: str) -> datetime:
    """Add one timedelta without allowing Python or DateTime64 overflow."""
    try:
        timestamp_with_interval = timestamp + interval
    except OverflowError as error:
        msg = f"{field} exceeds the supported datetime range"
        raise ValueError(msg) from error
    return require_datetime64(timestamp_with_interval, field=field)


def _is_utc(candidate: datetime) -> bool:
    if candidate.tzinfo is None:
        return False
    return candidate.utcoffset() == timedelta(0)
