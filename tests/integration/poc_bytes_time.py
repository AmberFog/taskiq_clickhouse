"""Real-ClickHouse POC actions for opaque bytes and server-owned time."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from importlib.metadata import version as distribution_version
import json
from typing import TYPE_CHECKING, Final, cast


if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

    from clickhouse_connect.driver.asyncclient import AsyncClient
    from clickhouse_connect.driver.query import QueryResult

    from tests.integration.settings import ClickHouseTestSettings


MEBIBYTE: Final = 1024**2
LARGE_PAYLOAD_SIZE: Final = 3 * MEBIBYTE
MICROSECOND: Final = timedelta(microseconds=1)
RESULT_TTL: Final = timedelta(hours=1)
PURGE_TTL: Final = timedelta(days=1)
SIMULATED_ROLLBACK: Final = timedelta(days=1)
DATETIME64_MIN: Final = datetime(1900, 1, 1, tzinfo=UTC)
DATETIME64_MAX: Final = datetime(2299, 12, 31, 23, 59, 59, 999999, tzinfo=UTC)
DATETIME64_OUT_OF_RANGE: Final = datetime(2300, 1, 1, tzinfo=UTC)

_ALL_OCTETS: Final = bytes(range(256))
_LARGE_PAYLOAD: Final = (_ALL_OCTETS * ((LARGE_PAYLOAD_SIZE // len(_ALL_OCTETS)) + 1))[:LARGE_PAYLOAD_SIZE]

BYTE_CASE_ROWS: Final[tuple[tuple[int, bytes], ...]] = (
    (0, b""),
    (1, b"before\x00after"),
    (2, b"\xff\xfe\x80\xc0"),
    (3, _ALL_OCTETS),
    (4, _LARGE_PAYLOAD),
)

TIME_CASE_ROWS: Final[tuple[tuple[int, datetime], ...]] = (
    (0, DATETIME64_MIN),
    (1, datetime(2024, 2, 29, 12, 34, 56, 123456, tzinfo=UTC)),
    (2, DATETIME64_MAX),
)
TIME_PRECISION_CASE_ID: Final = 1
TIME_PRECISION_VALUE: Final = TIME_CASE_ROWS[TIME_PRECISION_CASE_ID][1]

_BYTES_TABLE: Final = "poc_bytes_roundtrip"
_TIME_TABLE: Final = "poc_datetime64"
_GENERATION_TABLE: Final = "poc_generation_allocator"
_GENERATION_SCOPE: Final = "poc"
_GENERATION_KEY: Final = "simulated-rollback"
_CLICKHOUSE_CONNECT_DISTRIBUTION: Final = "clickhouse-connect"
_ROW_WIDTH: Final = 2

_DROP_BYTES_TABLE: Final = "DROP TABLE IF EXISTS poc_bytes_roundtrip SYNC"
_CREATE_BYTES_TABLE: Final = """
CREATE TABLE poc_bytes_roundtrip
(
    case_id UInt8,
    payload String
)
ENGINE = MergeTree
ORDER BY case_id
"""
_SELECT_BYTES_DIRECT: Final = """
SELECT case_id, payload
FROM poc_bytes_roundtrip
ORDER BY case_id
"""
_SELECT_BYTES_ALIASED: Final = """
SELECT case_id, payload AS payload_alias
FROM poc_bytes_roundtrip
ORDER BY case_id
"""

_DROP_TIME_TABLE: Final = "DROP TABLE IF EXISTS poc_datetime64 SYNC"
_CREATE_TIME_TABLE: Final = """
CREATE TABLE poc_datetime64
(
    case_id UInt8,
    occurred_at DateTime64(6, 'UTC')
)
ENGINE = MergeTree
ORDER BY case_id
"""
_SELECT_TIME_VALUES: Final = """
SELECT case_id, occurred_at
FROM poc_datetime64
ORDER BY case_id
"""
_SELECT_TIME_BOUNDARY: Final = """
SELECT
    occurred_at = {exact:DateTime64(6, 'UTC')} AS exact_match,
    occurred_at > {before:DateTime64(6, 'UTC')} AS visible_before,
    occurred_at > {equal:DateTime64(6, 'UTC')} AS visible_at_equality,
    occurred_at > {after:DateTime64(6, 'UTC')} AS visible_after
FROM poc_datetime64
WHERE case_id = {case_id:UInt8}
"""
_SELECT_TYPED_TIME_PARAMETER: Final = "SELECT {value:DateTime64(6, 'UTC')}"

_DROP_GENERATION_TABLE: Final = "DROP TABLE IF EXISTS poc_generation_allocator SYNC"
_CREATE_GENERATION_TABLE: Final = """
CREATE TABLE poc_generation_allocator
(
    scope String,
    record_key String,
    generation_at DateTime64(6, 'UTC')
)
ENGINE = MergeTree
ORDER BY (scope, record_key, generation_at)
"""
_SELECT_SERVER_NOW: Final = "SELECT now64(6, 'UTC')"
_SELECT_GENERATION_ALLOCATION: Final = """
SELECT
    now64(6, 'UTC') AS written_at,
    maxOrNull(generation_at) AS latest_generation_at
FROM poc_generation_allocator
PREWHERE scope = {scope:String} AND record_key = {record_key:String}
"""

_DDL_SETTINGS: Final = {"wait_end_of_query": 1}
_WRITE_SETTINGS: Final = {
    "async_insert": 0,
    "wait_for_async_insert": 1,
    "wait_end_of_query": 1,
}


@dataclass(frozen=True, slots=True)
class BytesObservation:
    """Hold direct and aliased byte projections from the same stored rows."""

    direct_rows: tuple[tuple[int, bytes], ...]
    aliased_rows: tuple[tuple[int, bytes], ...]


@dataclass(frozen=True, slots=True)
class TimeObservation:
    """Hold DateTime64 round-trip, range and exclusive-boundary results."""

    rows: tuple[tuple[int, datetime], ...]
    exact_match: bool
    visible_before: bool
    visible_at_equality: bool
    visible_after: bool
    out_of_range_bind_rejected: bool
    out_of_range_insert_rejected: bool
    last_valid_visible_until: datetime
    last_valid_purge_at: datetime
    deadline_overflow_rejected: bool


@dataclass(frozen=True, slots=True)
class GenerationObservation:
    """Hold server time, stored maximum, allocation and derived deadlines."""

    seed_server_now: datetime
    written_at: datetime
    latest_generation_at: datetime
    generation_at: datetime
    visible_until: datetime
    purge_at: datetime


class PocValueError(RuntimeError):
    """Report an unexpected POC result type or shape."""


class DateTime64RangeError(ValueError):
    """Reject a timestamp that cannot be represented by DateTime64(6)."""


async def exercise_bytes_roundtrip(client: AsyncClient) -> BytesObservation:
    """Store opaque bytes and read direct and aliased projections as bytes."""
    await _recreate_table(client, _DROP_BYTES_TABLE, _CREATE_BYTES_TABLE)
    await client.insert(
        table=_BYTES_TABLE,
        data=BYTE_CASE_ROWS,
        column_names=("case_id", "payload"),
        column_type_names=("UInt8", "String"),
        settings=_WRITE_SETTINGS,
    )
    direct_result = await client.query(
        _SELECT_BYTES_DIRECT,
        column_formats={"payload": "bytes"},
    )
    aliased_result = await client.query(
        _SELECT_BYTES_ALIASED,
        column_formats={"payload_alias": "bytes"},
    )
    return BytesObservation(
        direct_rows=_parse_bytes_rows(direct_result.result_rows),
        aliased_rows=_parse_bytes_rows(aliased_result.result_rows),
    )


async def exercise_datetime64(client: AsyncClient) -> TimeObservation:
    """Prove aware DateTime64 range, precision, typed binds and exclusivity."""
    await _recreate_table(client, _DROP_TIME_TABLE, _CREATE_TIME_TABLE)
    await _insert_time_rows(client, TIME_CASE_ROWS)
    time_result = await client.query(_SELECT_TIME_VALUES)
    boundary_result = await _query_time_boundary(
        client,
        case_id=TIME_PRECISION_CASE_ID,
        value=TIME_PRECISION_VALUE,
    )
    boundary_row = boundary_result.result_rows[0]
    out_of_range_bind_rejected = await _out_of_range_bind_is_rejected(client)
    out_of_range_insert_rejected = await _out_of_range_insert_is_rejected(client)
    last_valid_written_at = DATETIME64_MAX - PURGE_TTL
    last_valid_visible_until, last_valid_purge_at = _derive_deadlines(last_valid_written_at)
    return TimeObservation(
        rows=_parse_time_rows(time_result.result_rows),
        exact_match=bool(boundary_row[0]),
        visible_before=bool(boundary_row[1]),
        visible_at_equality=bool(boundary_row[2]),
        visible_after=bool(boundary_row[3]),
        out_of_range_bind_rejected=out_of_range_bind_rejected,
        out_of_range_insert_rejected=out_of_range_insert_rejected,
        last_valid_visible_until=last_valid_visible_until,
        last_valid_purge_at=last_valid_purge_at,
        deadline_overflow_rejected=_deadline_overflow_is_rejected(last_valid_written_at + MICROSECOND),
    )


async def exercise_generation_allocator(client: AsyncClient) -> GenerationObservation:
    """Allocate after a stored future generation using server time only."""
    await _recreate_table(client, _DROP_GENERATION_TABLE, _CREATE_GENERATION_TABLE)
    server_now_result = await client.query(_SELECT_SERVER_NOW)
    server_now = _as_aware_datetime(server_now_result.result_rows[0][0])
    simulated_latest = server_now + SIMULATED_ROLLBACK
    await client.insert(
        table=_GENERATION_TABLE,
        data=((_GENERATION_SCOPE, _GENERATION_KEY, simulated_latest),),
        column_names=("scope", "record_key", "generation_at"),
        column_type_names=("String", "String", "DateTime64(6, 'UTC')"),
        settings=_WRITE_SETTINGS,
    )
    allocation_result = await client.query(
        _SELECT_GENERATION_ALLOCATION,
        parameters={"scope": _GENERATION_SCOPE, "record_key": _GENERATION_KEY},
    )
    allocation_row = allocation_result.result_rows[0]
    written_at = _as_aware_datetime(allocation_row[0])
    latest_generation_at = _as_aware_datetime(allocation_row[1])
    generation_at = _require_datetime64(max(written_at, latest_generation_at + MICROSECOND))
    visible_until, purge_at = _derive_deadlines(written_at)
    return GenerationObservation(
        seed_server_now=server_now,
        written_at=written_at,
        latest_generation_at=latest_generation_at,
        generation_at=generation_at,
        visible_until=visible_until,
        purge_at=purge_at,
    )


async def write_bytes_evidence(
    settings: ClickHouseTestSettings,
    observation: BytesObservation,
) -> None:
    """Persist byte lengths and equality evidence without opaque payloads."""
    payload: dict[str, object] = {
        **_evidence_context(settings),
        "case_lengths": [len(value) for _case_id, value in BYTE_CASE_ROWS],
        "direct_match": observation.direct_rows == BYTE_CASE_ROWS,
        "aliased_match": observation.aliased_rows == BYTE_CASE_ROWS,
    }
    await _write_evidence(settings, "poc-bytes.json", payload)


async def write_time_evidence(
    settings: ClickHouseTestSettings,
    observation: TimeObservation,
) -> None:
    """Persist DateTime64 values and exclusive-boundary evidence."""
    payload: dict[str, object] = {
        **_evidence_context(settings),
        "roundtrip_values": [value.isoformat() for _case_id, value in observation.rows],
        "exact_match": observation.exact_match,
        "visible_before": observation.visible_before,
        "visible_at_equality": observation.visible_at_equality,
        "visible_after": observation.visible_after,
        "out_of_range_value": DATETIME64_OUT_OF_RANGE.isoformat(),
        "out_of_range_bind_rejected": observation.out_of_range_bind_rejected,
        "out_of_range_insert_rejected": observation.out_of_range_insert_rejected,
        "last_valid_visible_until": observation.last_valid_visible_until.isoformat(),
        "last_valid_purge_at": observation.last_valid_purge_at.isoformat(),
        "deadline_overflow_rejected": observation.deadline_overflow_rejected,
    }
    await _write_evidence(settings, "poc-time.json", payload)


async def write_generation_evidence(
    settings: ClickHouseTestSettings,
    observation: GenerationObservation,
) -> None:
    """Persist server-owned allocation and deadline evidence."""
    payload: dict[str, object] = {
        **_evidence_context(settings),
        "seed_server_now": observation.seed_server_now.isoformat(),
        "written_at": observation.written_at.isoformat(),
        "latest_generation_at": observation.latest_generation_at.isoformat(),
        "generation_at": observation.generation_at.isoformat(),
        "visible_until": observation.visible_until.isoformat(),
        "purge_at": observation.purge_at.isoformat(),
    }
    await _write_evidence(settings, "poc-generation.json", payload)


async def _insert_time_rows(
    client: AsyncClient,
    rows: Sequence[tuple[int, datetime]],
) -> None:
    validated_rows = tuple((case_id, _require_datetime64(value)) for case_id, value in rows)
    await client.insert(
        table=_TIME_TABLE,
        data=validated_rows,
        column_names=("case_id", "occurred_at"),
        column_type_names=("UInt8", "DateTime64(6, 'UTC')"),
        settings=_WRITE_SETTINGS,
    )


async def _query_time_boundary(
    client: AsyncClient,
    *,
    case_id: int,
    value: datetime,
) -> QueryResult:
    return await client.query(
        _SELECT_TIME_BOUNDARY,
        parameters={
            "case_id": case_id,
            "exact": _require_datetime64(value),
            "before": _require_datetime64(value - MICROSECOND),
            "equal": _require_datetime64(value),
            "after": _require_datetime64(value + MICROSECOND),
        },
    )


async def _query_typed_time_parameter(client: AsyncClient, value: datetime) -> None:
    await client.query(
        _SELECT_TYPED_TIME_PARAMETER,
        parameters={"value": _require_datetime64(value)},
    )


async def _out_of_range_bind_is_rejected(client: AsyncClient) -> bool:
    try:
        await _query_typed_time_parameter(client, DATETIME64_OUT_OF_RANGE)
    except DateTime64RangeError:
        return True
    return False


async def _out_of_range_insert_is_rejected(client: AsyncClient) -> bool:
    try:
        await _insert_time_rows(client, ((255, DATETIME64_OUT_OF_RANGE),))
    except DateTime64RangeError:
        return True
    return False


def _derive_deadlines(written_at: datetime) -> tuple[datetime, datetime]:
    valid_written_at = _require_datetime64(written_at)
    try:
        visible_until = valid_written_at + RESULT_TTL
        purge_at = valid_written_at + PURGE_TTL
    except OverflowError as error:
        message = "deadline exceeds Python datetime range"
        raise DateTime64RangeError(message) from error
    return _require_datetime64(visible_until), _require_datetime64(purge_at)


def _deadline_overflow_is_rejected(written_at: datetime) -> bool:
    try:
        _derive_deadlines(written_at)
    except DateTime64RangeError:
        return True
    return False


async def _recreate_table(client: AsyncClient, drop_query: str, create_query: str) -> None:
    await client.command(drop_query, settings=_DDL_SETTINGS)
    await client.command(create_query, settings=_DDL_SETTINGS)


def _parse_bytes_rows(rows: Sequence[Sequence[object]]) -> tuple[tuple[int, bytes], ...]:
    parsed_rows: list[tuple[int, bytes]] = []
    for row in rows:
        if len(row) != _ROW_WIDTH or not isinstance(row[1], bytes):
            message = "String bytes projection returned an unexpected row"
            raise PocValueError(message)
        parsed_rows.append((int(cast("int", row[0])), row[1]))
    return tuple(parsed_rows)


def _parse_time_rows(rows: Sequence[Sequence[object]]) -> tuple[tuple[int, datetime], ...]:
    parsed_rows: list[tuple[int, datetime]] = []
    for row in rows:
        if len(row) != _ROW_WIDTH:
            message = "DateTime64 projection returned an unexpected row"
            raise PocValueError(message)
        parsed_rows.append((int(cast("int", row[0])), _as_aware_datetime(row[1])))
    return tuple(parsed_rows)


def _as_aware_datetime(value: object) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        message = "DateTime64 projection was not an aware datetime"
        raise PocValueError(message)
    return value


def _require_datetime64(value: datetime) -> datetime:
    if value.tzinfo is None:
        message = "DateTime64 value must be timezone-aware"
        raise DateTime64RangeError(message)
    utc_value = value.astimezone(UTC)
    if not DATETIME64_MIN <= utc_value <= DATETIME64_MAX:
        message = (
            f"DateTime64(6, 'UTC') value must be between {DATETIME64_MIN.isoformat()} and {DATETIME64_MAX.isoformat()}"
        )
        raise DateTime64RangeError(message)
    return utc_value


def _evidence_context(settings: ClickHouseTestSettings) -> dict[str, object]:
    return {
        "profile": settings.profile,
        "expected_version": settings.expected_version,
        "client_version": distribution_version(_CLICKHOUSE_CONNECT_DISTRIBUTION),
        "expected_client_version": settings.expected_client_version,
    }


async def _write_evidence(
    settings: ClickHouseTestSettings,
    filename: str,
    payload: dict[str, object],
) -> None:
    path = settings.evidence_dir / filename
    await asyncio.to_thread(_write_json, path, payload)


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
