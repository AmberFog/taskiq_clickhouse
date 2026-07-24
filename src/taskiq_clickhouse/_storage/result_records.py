"""Immutable result, tombstone and result-selection domain values."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

from taskiq_clickhouse._datetime64 import add_microseconds, require_datetime64
from taskiq_clickhouse._identifiers import QualifiedTable
from taskiq_clickhouse._storage import generation, record_validation
from taskiq_clickhouse._storage_policy import RetentionPolicy


if TYPE_CHECKING:
    from datetime import datetime
    from uuid import UUID


RESULT_STATE: Final = 0
TOMBSTONE_STATE: Final = 1

_OBSERVED_AT_FIELD: Final = "observed_at"
_PURGE_AT_FIELD: Final = "purge_at"


@dataclass(frozen=True, slots=True, repr=False)
class ResultRecord:
    """One immutable result or generation-targeted tombstone row."""

    namespace: str
    task_id: str
    generation_at: datetime
    generation_id: UUID
    state: int
    written_at: datetime
    visible_until: datetime
    purge_at: datetime
    result_payload: bytes
    log_payload: bytes

    def __post_init__(self) -> None:
        """Validate the complete row before any native insert attempt."""
        record_validation.require_namespace(self.namespace)
        record_validation.require_text(self.task_id, field="task_id")
        generation.Generation(self.generation_at, self.generation_id)
        _require_state(self.state)
        generation.require_write_times(self.written_at, self.visible_until, self.purge_at)
        record_validation.require_bytes(self.result_payload, field="result_payload")
        record_validation.require_bytes(self.log_payload, field="log_payload")
        if self.state == TOMBSTONE_STATE:
            _require_empty_tombstone_payloads(self.result_payload, self.log_payload)

    def as_row(self) -> tuple[object, ...]:
        """Return values in the exact migration-v1 result column order."""
        return (  # noqa: WPS227 - exact ten-column native insert contract.
            self.namespace,
            self.task_id,
            self.generation_at,
            self.generation_id,
            self.state,
            self.written_at,
            self.visible_until,
            self.purge_at,
            self.result_payload,
            self.log_payload,
        )


@dataclass(frozen=True, slots=True)
class ResultStateRead:
    """Payload-free latest result state with validated retention deadlines."""

    observed_at: datetime
    generation_at: datetime
    generation_id: UUID
    state: int
    visible_until: datetime
    purge_at: datetime

    def __post_init__(self) -> None:
        """Validate the exact readiness projection."""
        require_datetime64(self.observed_at, field=_OBSERVED_AT_FIELD)
        generation.Generation(self.generation_at, self.generation_id)
        _require_state(self.state)
        generation.Deadlines(self.visible_until, self.purge_at)

    @property
    def is_ready(self) -> bool:
        """Return whether the selected latest state is a visible result."""
        return self.state == RESULT_STATE and self.visible_until > self.observed_at


@dataclass(frozen=True, slots=True, repr=False)
class ResultPoint:
    """Bind one selected result to its exact logical and physical scope."""

    namespace: str
    task_id: str
    result_table: QualifiedTable

    def __post_init__(self) -> None:
        """Validate keys and require a package-qualified source table."""
        record_validation.require_namespace(self.namespace)
        record_validation.require_text(self.task_id, field="task_id")
        record_validation.require_instance(self.result_table, QualifiedTable, field="result table")


@dataclass(frozen=True, slots=True, repr=False)
class ResultRead:
    """Latest result projection with an optional requested log payload."""

    point: ResultPoint
    observed_at: datetime
    generation_at: datetime
    generation_id: UUID
    state: int
    visible_until: datetime
    purge_at: datetime
    result_payload: bytes
    log_payload: bytes | None = None

    def __post_init__(self) -> None:
        """Validate result identity, state, deadlines and opaque payload bytes."""
        record_validation.require_instance(self.point, ResultPoint, field="result point")
        require_datetime64(self.observed_at, field=_OBSERVED_AT_FIELD)
        generation.Generation(self.generation_at, self.generation_id)
        _require_state(self.state)
        generation.Deadlines(self.visible_until, self.purge_at)
        record_validation.require_bytes(self.result_payload, field="result_payload")
        if self.log_payload is not None:
            record_validation.require_bytes(self.log_payload, field="log_payload")
        if self.state == TOMBSTONE_STATE:
            _require_empty_tombstone_payloads(self.result_payload, self.log_payload)

    @property
    def is_visible_result(self) -> bool:
        """Return whether the latest row is a result inside its logical TTL."""
        return self.state == RESULT_STATE and self.visible_until > self.observed_at


def build_tombstone(selected: ResultRead, retention: RetentionPolicy) -> ResultRecord:
    """Build a finite tombstone for one already-decoded visible generation."""
    record_validation.require_instance(selected, ResultRead, field="selected result")
    if not selected.is_visible_result:
        msg = "only a visible result can be consumed"
        raise ValueError(msg)
    record_validation.require_instance(retention, RetentionPolicy, field="retention")
    retention_floor = add_microseconds(
        selected.observed_at,
        retention.purge_ttl_us,
        field=_PURGE_AT_FIELD,
    )
    return ResultRecord(
        namespace=selected.point.namespace,
        task_id=selected.point.task_id,
        generation_at=selected.generation_at,
        generation_id=selected.generation_id,
        state=TOMBSTONE_STATE,
        written_at=selected.observed_at,
        visible_until=selected.visible_until,
        purge_at=max(selected.purge_at, retention_floor),
        result_payload=b"",
        log_payload=b"",
    )


def _require_state(candidate: object) -> int:
    if type(candidate) is not int:  # noqa: WPS516 - booleans are not storage states.
        msg = "state must be an integer"
        raise TypeError(msg)
    if candidate not in {RESULT_STATE, TOMBSTONE_STATE}:
        msg = "state must be result 0 or tombstone 1"
        raise ValueError(msg)
    return candidate


def _require_empty_tombstone_payloads(result_payload: bytes, log_payload: bytes | None) -> None:
    if result_payload or log_payload:
        msg = "tombstone payloads must be empty bytes"
        raise ValueError(msg)
