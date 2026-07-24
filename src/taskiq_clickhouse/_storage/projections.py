"""Fail-closed decoders for untrusted ClickHouse storage projections."""

from collections.abc import Callable, Sequence
from datetime import datetime
from typing import Final, TypeAlias, TypeVar
from uuid import UUID

from taskiq_clickhouse._storage import generation, progress_records, result_records, row_shape
from taskiq_clickhouse.exceptions import (
    ClickHouseDataCorruptionError,
    ClickHouseProgressError,
)


_RESULT_STATE_WIDTH: Final = 6
_RESULT_READ_WIDTH: Final = 7
_RESULT_WITH_LOG_WIDTH: Final = 8
_PROGRESS_READ_WIDTH: Final = 6
_GENERATION_READ_WIDTH: Final = 3
_CONFIRMATION_WIDTH: Final = 1
_OBSERVED_AT_TYPE_ERROR: Final = "observed_at must be a datetime"
_PROJECTION_CORRUPT: Final = "projection_corrupt"

_ProjectionRows: TypeAlias = Sequence[Sequence[object]]
CorruptionType: TypeAlias = type[ClickHouseDataCorruptionError] | type[ClickHouseProgressError]
_Projection = TypeVar("_Projection")


def decode_projection(
    parser: Callable[[_ProjectionRows], _Projection],
    rows: _ProjectionRows,
    *,
    operation: str,
    error_type: CorruptionType,
) -> _Projection:
    """Translate an untrusted projection failure into a detached safe error."""
    parsed: tuple[_Projection, ...] = ()
    try:
        parsed = (parser(rows),)
    except (TypeError, ValueError):
        parsed = ()
    if not parsed:
        raise error_type(operation, _PROJECTION_CORRUPT) from None
    return parsed[0]


def parse_generation_row(rows: row_shape.Rows) -> generation.GenerationRead:
    """Decode one allocator row and its coherent nullable historical pair."""
    row = row_shape.required_row(rows, width=_GENERATION_READ_WIDTH, projection="generation")
    return generation.GenerationRead(
        written_at=row_shape.typed_value(
            row[0],
            datetime,
            error="written_at must be a datetime",
        ),
        latest_generation_at=row_shape.optional_typed_value(
            row[1],
            datetime,
            error="latest_generation_at must be a datetime",
        ),
        latest_purge_at=row_shape.optional_typed_value(
            row[2],
            datetime,
            error="latest_purge_at must be a datetime",
        ),
    )


def parse_result_state_rows(rows: row_shape.Rows) -> result_records.ResultStateRead | None:
    """Decode zero or one exact metadata-only readiness row."""
    row = row_shape.optional_row(rows, width=_RESULT_STATE_WIDTH, projection="result state")
    if row is None:
        return None
    return result_records.ResultStateRead(
        observed_at=row_shape.typed_value(
            row[0],
            datetime,
            error=_OBSERVED_AT_TYPE_ERROR,
        ),
        generation_at=row_shape.typed_value(
            row[1],
            datetime,
            error="generation_at must be a datetime",
        ),
        generation_id=row_shape.typed_value(
            row[2],
            UUID,
            error="generation_id must be a UUID",
        ),
        state=row_shape.typed_value(
            row[3],
            int,
            error="state must be an integer",
        ),
        visible_until=row_shape.typed_value(
            row[4],
            datetime,
            error="visible_until must be a datetime",
        ),
        purge_at=row_shape.typed_value(
            row[5],
            datetime,
            error="purge_at must be a datetime",
        ),
    )


def parse_result_rows(
    rows: row_shape.Rows,
    *,
    point: result_records.ResultPoint,
    with_logs: object,
) -> result_records.ResultRead | None:
    """Decode zero or one latest result row for the requested projection."""
    _require_result_point(point)
    if type(with_logs) is not bool:  # noqa: WPS516 - projection shape requires an exact boolean.
        msg = "with_logs must be a boolean"
        raise TypeError(msg)
    width = _RESULT_WITH_LOG_WIDTH if with_logs else _RESULT_READ_WIDTH
    row = row_shape.optional_row(rows, width=width, projection="result")
    if row is None:
        return None
    log_payload = (
        row_shape.typed_value(
            row[7],
            bytes,
            error="log_payload must be bytes",
        )
        if with_logs
        else None
    )
    return result_records.ResultRead(
        point=point,
        observed_at=row_shape.typed_value(
            row[0],
            datetime,
            error=_OBSERVED_AT_TYPE_ERROR,
        ),
        generation_at=row_shape.typed_value(
            row[1],
            datetime,
            error="generation_at must be a datetime",
        ),
        generation_id=row_shape.typed_value(
            row[2],
            UUID,
            error="generation_id must be a UUID",
        ),
        state=row_shape.typed_value(
            row[3],
            int,
            error="state must be an integer",
        ),
        visible_until=row_shape.typed_value(
            row[4],
            datetime,
            error="visible_until must be a datetime",
        ),
        purge_at=row_shape.typed_value(
            row[5],
            datetime,
            error="purge_at must be a datetime",
        ),
        result_payload=row_shape.typed_value(
            row[6],
            bytes,
            error="result_payload must be bytes",
        ),
        log_payload=log_payload,
    )


def parse_progress_rows(rows: row_shape.Rows) -> progress_records.ProgressRead | None:
    """Decode zero or one exact latest-progress row."""
    row = row_shape.optional_row(rows, width=_PROGRESS_READ_WIDTH, projection="progress")
    if row is None:
        return None
    return progress_records.ProgressRead(
        observed_at=row_shape.typed_value(
            row[0],
            datetime,
            error=_OBSERVED_AT_TYPE_ERROR,
        ),
        generation_at=row_shape.typed_value(
            row[1],
            datetime,
            error="generation_at must be a datetime",
        ),
        generation_id=row_shape.typed_value(
            row[2],
            UUID,
            error="generation_id must be a UUID",
        ),
        visible_until=row_shape.typed_value(
            row[3],
            datetime,
            error="visible_until must be a datetime",
        ),
        purge_at=row_shape.typed_value(
            row[4],
            datetime,
            error="purge_at must be a datetime",
        ),
        progress_payload=row_shape.typed_value(
            row[5],
            bytes,
            error="progress_payload must be bytes",
        ),
    )


def parse_confirmation_rows(rows: row_shape.Rows) -> bool:
    """Decode exact-identity confirmation as absent or one literal-one row."""
    row = row_shape.optional_row(rows, width=_CONFIRMATION_WIDTH, projection="confirmation")
    if row is None:
        return False
    confirmation = row[0]
    if type(confirmation) is not int or confirmation != 1:  # noqa: WPS516 - reject booleans.
        msg = "confirmation must return literal 1"
        raise ValueError(msg)
    return True


def _require_result_point(candidate: object) -> None:
    if not isinstance(candidate, result_records.ResultPoint):
        msg = "result point must be a ResultPoint"
        raise TypeError(msg)
