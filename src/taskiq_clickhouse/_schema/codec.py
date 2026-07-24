"""Strict decoding for append-only metadata rows."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import TYPE_CHECKING, cast

from taskiq_clickhouse._schema.records import MetadataRecord
from taskiq_clickhouse.exceptions import ClickHouseMigrationError


if TYPE_CHECKING:
    from collections.abc import Sequence
    from uuid import UUID


_METADATA_ROW_WIDTH = 10
_CLOCK_OPERATION = "metadata_clock_read"
_CLOCK_INVALID = "clock_invalid"


def parse_records(
    rows: Sequence[Sequence[object]],
    *,
    operation: str,
) -> tuple[MetadataRecord, ...]:
    """Decode complete rows or expose only a safe corruption code."""
    try:
        parsed = tuple(_parse_record(row) for row in rows)
    except (IndexError, TypeError, ValueError):
        invalid = True
    else:
        invalid = False
    if invalid:
        reason = "record_corrupt"
        raise ClickHouseMigrationError(operation, reason) from None
    return parsed


def parse_server_time(rows: Sequence[Sequence[object]]) -> datetime:
    """Require one aware datetime; MetadataRecord validates UTC later."""
    observed_at = _server_time(rows)
    if observed_at is None:
        raise ClickHouseMigrationError(_CLOCK_OPERATION, _CLOCK_INVALID) from None
    return observed_at


def _server_time(rows: Sequence[Sequence[object]]) -> datetime | None:
    if len(rows) != 1:
        return None
    row = rows[0]
    if len(row) != 1:
        return None
    observed_at = row[0]
    if not isinstance(observed_at, datetime) or observed_at.tzinfo is None:
        return None
    if observed_at.utcoffset() != timedelta(0):
        return None
    return observed_at


def _parse_record(row: Sequence[object]) -> MetadataRecord:
    if len(row) != _METADATA_ROW_WIDTH:
        msg = "metadata row has the wrong width"
        raise ValueError(msg)
    return MetadataRecord(
        record_kind=_decode_text(row[0]),
        scope=_decode_text(row[1]),
        record_key=_decode_text(row[2]),
        version=_exact_int(row[3]),
        name=_decode_text(row[4]),
        payload=_exact_bytes(row[5]),
        checksum=_decode_text(row[6]),
        package_version=_decode_text(row[7]),
        recorded_at=cast("datetime", row[8]),
        attempt_id=cast("UUID", row[9]),
    )


def _decode_text(raw_value: object) -> str:
    if not isinstance(raw_value, bytes):
        msg = "metadata String was not returned as bytes"
        raise TypeError(msg)
    return raw_value.decode("utf-8", errors="strict")


def _exact_bytes(raw_value: object) -> bytes:
    if not isinstance(raw_value, bytes):
        msg = "metadata payload was not returned as bytes"
        raise TypeError(msg)
    return raw_value


def _exact_int(raw_value: object) -> int:
    if not isinstance(raw_value, int) or isinstance(raw_value, bool):
        msg = "metadata version was not returned as int"
        raise TypeError(msg)
    return raw_value
