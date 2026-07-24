"""Immutable progress write and selection domain values."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

from taskiq_clickhouse._datetime64 import require_datetime64
from taskiq_clickhouse._storage import generation, record_validation


if TYPE_CHECKING:
    from datetime import datetime
    from uuid import UUID


_OBSERVED_AT_FIELD: Final = "observed_at"


@dataclass(frozen=True, slots=True, repr=False)
class ProgressRecord:
    """One immutable progress update row."""

    namespace: str
    task_id: str
    generation_at: datetime
    generation_id: UUID
    written_at: datetime
    visible_until: datetime
    purge_at: datetime
    progress_payload: bytes

    def __post_init__(self) -> None:
        """Validate the complete row before any native insert attempt."""
        record_validation.require_namespace(self.namespace)
        record_validation.require_text(self.task_id, field="task_id")
        generation.Generation(self.generation_at, self.generation_id)
        generation.require_write_times(self.written_at, self.visible_until, self.purge_at)
        record_validation.require_bytes(self.progress_payload, field="progress_payload")

    def as_row(self) -> tuple[object, ...]:
        """Return values in the exact migration-v1 progress column order."""
        return (  # noqa: WPS227 - exact eight-column native insert contract.
            self.namespace,
            self.task_id,
            self.generation_at,
            self.generation_id,
            self.written_at,
            self.visible_until,
            self.purge_at,
            self.progress_payload,
        )


@dataclass(frozen=True, slots=True, repr=False)
class ProgressRead:
    """Latest progress projection and its ClickHouse observation time."""

    observed_at: datetime
    generation_at: datetime
    generation_id: UUID
    visible_until: datetime
    purge_at: datetime
    progress_payload: bytes

    def __post_init__(self) -> None:
        """Validate progress identity, deadlines and opaque payload bytes."""
        require_datetime64(self.observed_at, field=_OBSERVED_AT_FIELD)
        generation.Generation(self.generation_at, self.generation_id)
        generation.Deadlines(self.visible_until, self.purge_at)
        record_validation.require_bytes(self.progress_payload, field="progress_payload")

    @property
    def is_visible(self) -> bool:
        """Return whether progress remains logically available."""
        return self.visible_until > self.observed_at
