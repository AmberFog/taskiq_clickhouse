"""Generation ordering and finite-retention domain rules."""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from uuid import UUID, uuid4

from taskiq_clickhouse._datetime64 import (
    add_interval,
    add_microseconds,
    require_datetime64,
)
from taskiq_clickhouse._storage import record_validation
from taskiq_clickhouse._storage_policy import RetentionPolicy
from taskiq_clickhouse._uuid4 import require_uuid4


_MICROSECOND = timedelta(microseconds=1)
_GENERATION_AT_FIELD = "generation_at"
_GENERATION_ID_FIELD = "generation_id"
_WRITTEN_AT_FIELD = "written_at"
_VISIBLE_UNTIL_FIELD = "visible_until"
_PURGE_AT_FIELD = "purge_at"

UUIDFactory = Callable[[], UUID]


@dataclass(frozen=True, slots=True)
class Generation:
    """One immutable total-order generation identity."""

    generation_at: datetime
    generation_id: UUID

    def __post_init__(self) -> None:
        """Require an in-range UTC timestamp and a random RFC 4122 UUIDv4."""
        require_datetime64(self.generation_at, field=_GENERATION_AT_FIELD)
        require_uuid4(self.generation_id, field=_GENERATION_ID_FIELD)


@dataclass(frozen=True, slots=True)
class Deadlines:
    """Logical visibility and later physical-purge deadlines."""

    visible_until: datetime
    purge_at: datetime

    def __post_init__(self) -> None:
        """Require finite UTC deadlines in their strict storage order."""
        require_datetime64(self.visible_until, field=_VISIBLE_UNTIL_FIELD)
        require_datetime64(self.purge_at, field=_PURGE_AT_FIELD)
        if self.visible_until >= self.purge_at:
            msg = "visible_until must be earlier than purge_at"
            raise ValueError(msg)


@dataclass(frozen=True, slots=True)
class GenerationRead:
    """Server time and optional retention history from an allocator query."""

    written_at: datetime
    latest_generation_at: datetime | None
    latest_purge_at: datetime | None

    def __post_init__(self) -> None:
        """Validate server time and one coherent nullable historical pair."""
        require_datetime64(self.written_at, field=_WRITTEN_AT_FIELD)
        if (self.latest_generation_at is None) is not (self.latest_purge_at is None):
            msg = "latest generation and purge timestamps must both be null or both be present"
            raise ValueError(msg)
        if self.latest_generation_at is not None:
            require_datetime64(self.latest_generation_at, field="latest_generation_at")
        if self.latest_purge_at is not None:
            require_datetime64(self.latest_purge_at, field="latest_purge_at")


@dataclass(frozen=True, slots=True)
class WriteAllocation:
    """Frozen server-owned write time, generation and retention deadlines."""

    written_at: datetime
    generation: Generation
    deadlines: Deadlines

    def __post_init__(self) -> None:
        """Reject manually assembled allocations that violate write invariants."""
        require_datetime64(self.written_at, field=_WRITTEN_AT_FIELD)
        record_validation.require_instance(self.generation, Generation, field="generation")
        record_validation.require_instance(self.deadlines, Deadlines, field="deadlines")
        if self.generation.generation_at < self.written_at:
            msg = "generation_at must not precede written_at"
            raise ValueError(msg)
        if self.deadlines.visible_until <= self.written_at:
            msg = "visible_until must be later than written_at"
            raise ValueError(msg)


def allocate_write(
    observation: GenerationRead,
    retention: RetentionPolicy,
    *,
    uuid_factory: UUIDFactory = uuid4,
) -> WriteAllocation:
    """Allocate one frozen generation from server time and stored history."""
    record_validation.require_instance(observation, GenerationRead, field="generation observation")
    record_validation.require_instance(retention, RetentionPolicy, field="retention")
    deadlines = derive_deadlines(observation.written_at, retention)
    if observation.latest_purge_at is not None:
        deadlines = Deadlines(
            deadlines.visible_until,
            max(deadlines.purge_at, observation.latest_purge_at),
        )
    generation_at = observation.written_at
    if observation.latest_generation_at is not None:
        latest_successor = add_interval(
            observation.latest_generation_at,
            _MICROSECOND,
            field=_GENERATION_AT_FIELD,
        )
        generation_at = max(generation_at, latest_successor)
    generation = Generation(generation_at, uuid_factory())
    return WriteAllocation(observation.written_at, generation, deadlines)


def derive_deadlines(written_at: datetime, retention: RetentionPolicy) -> Deadlines:
    """Compute finite ordered deadlines with exact integer microseconds."""
    require_datetime64(written_at, field=_WRITTEN_AT_FIELD)
    record_validation.require_instance(retention, RetentionPolicy, field="retention")
    visible_until = add_microseconds(
        written_at,
        retention.result_ttl_us,
        field=_VISIBLE_UNTIL_FIELD,
    )
    purge_at = add_microseconds(
        written_at,
        retention.purge_ttl_us,
        field=_PURGE_AT_FIELD,
    )
    return Deadlines(visible_until, purge_at)


def require_write_times(written_at: object, visible_until: object, purge_at: object) -> None:
    """Require one write time followed by finite ordered deadlines."""
    written = require_datetime64(written_at, field=_WRITTEN_AT_FIELD)
    deadlines = Deadlines(
        require_datetime64(visible_until, field=_VISIBLE_UNTIL_FIELD),
        require_datetime64(purge_at, field=_PURGE_AT_FIELD),
    )
    if deadlines.visible_until <= written:
        msg = "visible_until must be later than written_at"
        raise ValueError(msg)
