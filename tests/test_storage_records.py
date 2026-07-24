"""Validate immutable storage rows, time arithmetic and strict projections."""

from datetime import UTC, datetime, timedelta, timezone
from typing import Any, cast
from uuid import UUID

import pytest

from taskiq_clickhouse._datetime64 import DATETIME64_MAX, DATETIME64_MIN
from taskiq_clickhouse._identifiers import Identifier, QualifiedTable
from taskiq_clickhouse._storage.generation import (
    Deadlines,
    Generation,
    GenerationRead,
    WriteAllocation,
    allocate_write,
    derive_deadlines,
)
from taskiq_clickhouse._storage.progress_records import ProgressRead, ProgressRecord
from taskiq_clickhouse._storage.projections import (
    parse_confirmation_rows,
    parse_generation_row,
    parse_progress_rows,
    parse_result_rows,
    parse_result_state_rows,
)
from taskiq_clickhouse._storage.result_records import (
    RESULT_STATE,
    TOMBSTONE_STATE,
    ResultPoint,
    ResultRead,
    ResultRecord,
    ResultStateRead,
    build_tombstone,
)
from taskiq_clickhouse._storage_policy import RetentionPolicy


NOW = datetime(2026, 7, 15, 12, 30, 0, 123456, tzinfo=UTC)
VISIBLE = NOW + timedelta(hours=1)
PURGE = NOW + timedelta(days=1)
GENERATION_ID = UUID("00000000-0000-4000-8000-000000000001")
OTHER_GENERATION_ID = UUID("ffffffff-ffff-4fff-bfff-ffffffffffff")
UUID_V1 = UUID("00000000-0000-1000-8000-000000000001")
RESULT_TTL_US = 3_600_000_000
PURGE_TTL_US = 86_400_000_000
RETENTION = RetentionPolicy(RESULT_TTL_US, PURGE_TTL_US)
MICROSECOND = timedelta(microseconds=1)
RESULT_TABLE = QualifiedTable(Identifier("tasks"), Identifier("results"))
RESULT_POINT = ResultPoint("namespace", "task", RESULT_TABLE)


def _result_record(*, state: int = RESULT_STATE) -> ResultRecord:
    payload = b"result" if state == RESULT_STATE else b""
    log = b"log" if state == RESULT_STATE else b""
    return ResultRecord(
        namespace="namespace",
        task_id="task\x00id",
        generation_at=NOW,
        generation_id=GENERATION_ID,
        state=state,
        written_at=NOW,
        visible_until=VISIBLE,
        purge_at=PURGE,
        result_payload=payload,
        log_payload=log,
    )


def _progress_record() -> ProgressRecord:
    return ProgressRecord(
        namespace="namespace",
        task_id="task",
        generation_at=NOW,
        generation_id=GENERATION_ID,
        written_at=NOW,
        visible_until=VISIBLE,
        purge_at=PURGE,
        progress_payload=b"progress",
    )


def _result_row(*, state: int = RESULT_STATE, with_logs: bool = False) -> tuple[object, ...]:
    values: tuple[object, ...] = (
        NOW,
        NOW,
        GENERATION_ID,
        state,
        VISIBLE,
        PURGE,
        b"result",
    )
    if with_logs:
        return (*values, b"log")
    return values


def _selected_result(
    *,
    state: int = RESULT_STATE,
    observed_at: datetime = NOW,
    visible_until: datetime = VISIBLE,
    purge_at: datetime = PURGE,
) -> ResultRead:
    payload = b"result" if state == RESULT_STATE else b""
    return ResultRead(
        point=RESULT_POINT,
        observed_at=observed_at,
        generation_at=NOW,
        generation_id=GENERATION_ID,
        state=state,
        visible_until=visible_until,
        purge_at=purge_at,
        result_payload=payload,
    )


def test_payload_and_task_storage_values_have_safe_representations() -> None:
    """Keep opaque payloads and task identifiers out of incidental diagnostics."""
    secret = "task-secret"  # noqa: S105  # pragma: allowlist secret
    result_record = ResultRecord(
        namespace="namespace",
        task_id=secret,
        generation_at=NOW,
        generation_id=GENERATION_ID,
        state=RESULT_STATE,
        written_at=NOW,
        visible_until=VISIBLE,
        purge_at=PURGE,
        result_payload=secret.encode(),
        log_payload=secret.encode(),
    )
    progress_record = ProgressRecord(
        namespace="namespace",
        task_id=secret,
        generation_at=NOW,
        generation_id=GENERATION_ID,
        written_at=NOW,
        visible_until=VISIBLE,
        purge_at=PURGE,
        progress_payload=secret.encode(),
    )
    point = ResultPoint("namespace", secret, RESULT_TABLE)
    result = ResultRead(
        point=point,
        observed_at=NOW,
        generation_at=NOW,
        generation_id=GENERATION_ID,
        state=RESULT_STATE,
        visible_until=VISIBLE,
        purge_at=PURGE,
        result_payload=secret.encode(),
        log_payload=secret.encode(),
    )
    progress = ProgressRead(
        observed_at=NOW,
        generation_at=NOW,
        generation_id=GENERATION_ID,
        visible_until=VISIBLE,
        purge_at=PURGE,
        progress_payload=secret.encode(),
    )

    values = (result_record, progress_record, point, result, progress)
    assert all(secret not in repr(value) for value in values)


def test_generation_deadlines_and_allocation_are_immutable_value_objects() -> None:
    """Freeze and validate the complete allocation result."""
    generation = Generation(NOW, GENERATION_ID)
    deadlines = Deadlines(VISIBLE, PURGE)
    allocation = WriteAllocation(NOW, generation, deadlines)

    assert allocation == WriteAllocation(NOW, generation, deadlines)
    assert hash(generation) == hash(Generation(NOW, GENERATION_ID))


@pytest.mark.parametrize(
    ("generation", "deadlines", "error_type", "match"),
    [
        (
            cast("Any", object()),
            Deadlines(VISIBLE, PURGE),
            TypeError,
            "generation must be a Generation",
        ),
        (
            Generation(NOW, GENERATION_ID),
            cast("Any", object()),
            TypeError,
            "deadlines must be a Deadlines",
        ),
        (
            Generation(NOW - MICROSECOND, GENERATION_ID),
            Deadlines(VISIBLE, PURGE),
            ValueError,
            "generation_at must not precede written_at",
        ),
        (
            Generation(NOW, GENERATION_ID),
            Deadlines(NOW, PURGE),
            ValueError,
            "visible_until must be later than written_at",
        ),
    ],
)
def test_write_allocation_rejects_manually_broken_invariants(
    generation: Generation,
    deadlines: Deadlines,
    error_type: type[Exception],
    match: str,
) -> None:
    """Reject invalid value objects assembled outside the allocator."""
    with pytest.raises(error_type, match=match):
        WriteAllocation(NOW, generation, deadlines)


@pytest.mark.parametrize(
    ("latest", "expected"),
    [
        (None, NOW),
        (NOW - timedelta(days=1), NOW),
        (NOW + timedelta(days=1), NOW + timedelta(days=1) + MICROSECOND),
    ],
)
def test_allocate_write_uses_server_time_and_stored_successor(
    latest: datetime | None,
    expected: datetime,
) -> None:
    """Use max(server now, latest plus one microsecond), never worker time."""
    latest_purge_at = None if latest is None else PURGE
    allocation = allocate_write(
        GenerationRead(NOW, latest, latest_purge_at),
        RETENTION,
        uuid_factory=lambda: OTHER_GENERATION_ID,
    )

    assert allocation.written_at == NOW
    assert allocation.generation == Generation(expected, OTHER_GENERATION_ID)
    assert allocation.deadlines == Deadlines(VISIBLE, PURGE)


def test_allocate_write_keeps_historical_purge_floor_after_server_rollback() -> None:
    """Keep new history until the latest retained row can no longer resurrect."""
    historical_generation = NOW + timedelta(hours=2)
    historical_purge = NOW + timedelta(days=3)

    allocation = allocate_write(
        GenerationRead(NOW, historical_generation, historical_purge),
        RETENTION,
        uuid_factory=lambda: OTHER_GENERATION_ID,
    )

    assert allocation.written_at == NOW
    assert allocation.generation.generation_at == historical_generation + MICROSECOND
    assert allocation.deadlines.visible_until == VISIBLE
    assert allocation.deadlines.purge_at == historical_purge


def test_allocate_write_rejects_generation_successor_overflow() -> None:
    """Fail before UUID allocation when stored history has no successor."""
    called = False

    def uuid_factory() -> UUID:
        nonlocal called
        called = True
        return GENERATION_ID

    with pytest.raises(ValueError, match="generation_at"):
        allocate_write(
            GenerationRead(NOW, DATETIME64_MAX, PURGE),
            RETENTION,
            uuid_factory=uuid_factory,
        )

    assert called is False


def test_deadlines_reject_datetime64_overflow_at_write_time() -> None:
    """Reject a valid interval that exceeds the remaining timestamp range."""
    with pytest.raises(ValueError, match="purge_at"):
        derive_deadlines(DATETIME64_MAX - MICROSECOND, RetentionPolicy(1, 2))


@pytest.mark.parametrize(
    ("timestamp", "error_type", "match"),
    [
        (cast("Any", "2026-01-01"), TypeError, "must be a datetime"),
        (NOW.replace(tzinfo=None), ValueError, "timezone-aware UTC"),
        (
            datetime(2026, 1, 1, tzinfo=timezone(timedelta(hours=1))),
            ValueError,
            "timezone-aware UTC",
        ),
        (DATETIME64_MIN - MICROSECOND, ValueError, "must fit DateTime64"),
        (DATETIME64_MAX + MICROSECOND, ValueError, "must fit DateTime64"),
    ],
)
def test_generation_rejects_invalid_datetime64_values(
    timestamp: datetime,
    error_type: type[Exception],
    match: str,
) -> None:
    """Require UTC and the frozen application DateTime64 range."""
    with pytest.raises(error_type, match=match):
        Generation(timestamp, GENERATION_ID)


@pytest.mark.parametrize(
    ("generation_id", "error_type", "match"),
    [
        (cast("Any", "uuid"), TypeError, "must be a UUID"),
        (UUID_V1, ValueError, "UUIDv4"),
    ],
)
def test_generation_rejects_non_uuid4_identities(
    generation_id: UUID,
    error_type: type[Exception],
    match: str,
) -> None:
    """Allow only the RFC 4122 UUIDv4 tie-breaker promised by the contract."""
    with pytest.raises(error_type, match=match):
        Generation(NOW, generation_id)


def test_result_and_progress_rows_match_migration_column_order() -> None:
    """Build exact native tuples without an implicit driver schema lookup."""
    result = _result_record()
    progress = _progress_record()

    assert result.as_row() == (
        "namespace",
        "task\x00id",
        NOW,
        GENERATION_ID,
        RESULT_STATE,
        NOW,
        VISIBLE,
        PURGE,
        b"result",
        b"log",
    )
    assert progress.as_row() == (
        "namespace",
        "task",
        NOW,
        GENERATION_ID,
        NOW,
        VISIBLE,
        PURGE,
        b"progress",
    )


@pytest.mark.parametrize("state", [True, -1, 2])
def test_result_record_rejects_unknown_or_non_integer_state(state: int) -> None:
    """Treat every value outside exact UInt8 states zero and one as invalid."""
    error_type = TypeError if state is True else ValueError
    with pytest.raises(error_type, match="state"):
        _result_record(state=state)


def test_result_record_requires_empty_tombstone_payloads() -> None:
    """Prevent tombstones from accidentally retaining serialized data or logs."""
    with pytest.raises(ValueError, match="tombstone payloads"):
        ResultRecord(
            namespace="namespace",
            task_id="task",
            generation_at=NOW,
            generation_id=GENERATION_ID,
            state=TOMBSTONE_STATE,
            written_at=NOW,
            visible_until=VISIBLE,
            purge_at=PURGE,
            result_payload=b"retained",
            log_payload=b"",
        )


@pytest.mark.parametrize(
    ("namespace", "task_id", "payload", "error_type", "match"),
    [
        (cast("Any", 1), "task", b"result", TypeError, "namespace"),
        ("bad namespace", "task", b"result", ValueError, "namespace"),
        ("namespace", cast("Any", 1), b"result", TypeError, "task_id"),
        ("namespace", "task", cast("Any", bytearray(b"result")), TypeError, "result_payload"),
    ],
)
def test_result_record_validates_keys_and_opaque_bytes(
    namespace: str,
    task_id: str,
    payload: bytes,
    error_type: type[Exception],
    match: str,
) -> None:
    """Validate stored keys and require exact immutable payload bytes."""
    with pytest.raises(error_type, match=match):
        ResultRecord(
            namespace=namespace,
            task_id=task_id,
            generation_at=NOW,
            generation_id=GENERATION_ID,
            state=RESULT_STATE,
            written_at=NOW,
            visible_until=VISIBLE,
            purge_at=PURGE,
            result_payload=payload,
            log_payload=b"log",
        )


@pytest.mark.parametrize(
    ("visible_until", "purge_at", "match"),
    [
        (NOW, PURGE, "later than written_at"),
        (VISIBLE, VISIBLE, "earlier than purge_at"),
        (PURGE, VISIBLE, "earlier than purge_at"),
    ],
)
def test_records_reject_unordered_deadlines(
    visible_until: datetime,
    purge_at: datetime,
    match: str,
) -> None:
    """Keep visibility strictly after write time and purge strictly later."""
    with pytest.raises(ValueError, match=match):
        ProgressRecord(
            namespace="namespace",
            task_id="task",
            generation_at=NOW,
            generation_id=GENERATION_ID,
            written_at=NOW,
            visible_until=visible_until,
            purge_at=purge_at,
            progress_payload=b"progress",
        )


def test_parse_generation_row_accepts_nullable_maximum() -> None:
    """Parse exactly one allocator row and one atomic nullable pair."""
    assert parse_generation_row(((NOW, None, None),)) == GenerationRead(NOW, None, None)
    assert parse_generation_row(((NOW, VISIBLE, PURGE),)) == GenerationRead(
        NOW,
        VISIBLE,
        PURGE,
    )


@pytest.mark.parametrize(
    ("latest_generation_at", "latest_purge_at"),
    [(None, PURGE), (VISIBLE, None)],
)
def test_generation_read_rejects_partial_historical_pairs(
    latest_generation_at: datetime | None,
    latest_purge_at: datetime | None,
) -> None:
    """Reject aggregate projections where only one historical maximum is null."""
    with pytest.raises(ValueError, match="both be null or both be present"):
        GenerationRead(NOW, latest_generation_at, latest_purge_at)


@pytest.mark.parametrize(
    ("rows", "error_type", "match"),
    [
        ((), ValueError, "exactly one row"),
        (((NOW, None, None), (NOW, None, None)), ValueError, "exactly one row"),
        (((NOW, None),), ValueError, "exactly 3 values"),
        ((cast("Any", "bad-row"),), TypeError, "row must be a sequence"),
        (cast("Any", object()), TypeError, "rows must be a sequence"),
        (((NOW, "latest", PURGE),), TypeError, "latest_generation_at must be a datetime"),
        (((NOW, VISIBLE, "purge"),), TypeError, "latest_purge_at must be a datetime"),
        (((NOW, None, PURGE),), ValueError, "both be null or both be present"),
    ],
)
def test_parse_generation_row_rejects_malformed_results(
    rows: tuple[tuple[object, ...], ...],
    error_type: type[Exception],
    match: str,
) -> None:
    """Reject missing, duplicate, malformed and mistyped allocator rows."""
    with pytest.raises(error_type, match=match):
        parse_generation_row(rows)


def test_parse_readiness_rows_preserves_latest_state_semantics() -> None:
    """Represent missing, visible, expired and tombstone latest states exactly."""
    assert parse_result_state_rows(()) is None
    ready = parse_result_state_rows(((NOW, NOW, GENERATION_ID, RESULT_STATE, VISIBLE, PURGE),))
    expired = ResultStateRead(VISIBLE, NOW, GENERATION_ID, RESULT_STATE, VISIBLE, PURGE)
    tombstone = ResultStateRead(NOW, NOW, GENERATION_ID, TOMBSTONE_STATE, VISIBLE, PURGE)

    assert ready is not None
    assert ready.is_ready is True
    assert expired.is_ready is False
    assert tombstone.is_ready is False


@pytest.mark.parametrize("purge_at", [VISIBLE, VISIBLE - MICROSECOND])
def test_parse_readiness_rows_rejects_corrupt_deadline_order(purge_at: datetime) -> None:
    """Fail closed when metadata-only readiness observes invalid retention."""
    rows = ((NOW, NOW, GENERATION_ID, RESULT_STATE, VISIBLE, purge_at),)

    with pytest.raises(ValueError, match="visible_until must be earlier than purge_at"):
        parse_result_state_rows(rows)


def test_parse_result_rows_uses_exact_no_log_and_with_log_shapes() -> None:
    """Keep omitted log distinguishable from serialized empty log bytes."""
    assert parse_result_rows((), point=RESULT_POINT, with_logs=False) is None
    no_log = parse_result_rows((_result_row(),), point=RESULT_POINT, with_logs=False)
    with_log = parse_result_rows(
        (_result_row(with_logs=True),),
        point=RESULT_POINT,
        with_logs=True,
    )

    assert no_log is not None
    assert no_log.log_payload is None
    assert with_log is not None
    assert with_log.log_payload == b"log"
    assert no_log.is_visible_result is True
    expired = ResultRead(
        point=RESULT_POINT,
        observed_at=VISIBLE,
        generation_at=NOW,
        generation_id=GENERATION_ID,
        state=RESULT_STATE,
        visible_until=VISIBLE,
        purge_at=PURGE,
        result_payload=b"",
    )
    assert expired.is_visible_result is False


def test_parse_result_rows_rejects_tombstones_that_retain_payloads() -> None:
    """Treat a non-empty tombstone payload as persisted-row corruption."""
    with pytest.raises(ValueError, match="tombstone payloads"):
        parse_result_rows(
            (_result_row(state=TOMBSTONE_STATE),),
            point=RESULT_POINT,
            with_logs=False,
        )
    with pytest.raises(ValueError, match="tombstone payloads"):
        parse_result_rows(
            (_result_row(state=TOMBSTONE_STATE, with_logs=True),),
            point=RESULT_POINT,
            with_logs=True,
        )


@pytest.mark.parametrize(
    ("rows", "with_logs", "error_type", "match"),
    [
        ((_result_row(), _result_row()), False, ValueError, "at most one row"),
        ((_result_row(with_logs=True),), False, ValueError, "exactly 7 values"),
        ((_result_row(),), True, ValueError, "exactly 8 values"),
        ((_result_row(state=2),), False, ValueError, "state"),
        (((*(_result_row()[:-1]), "text"),), False, TypeError, "result_payload"),
        (((*_result_row(with_logs=True)[:-1], "text"),), True, TypeError, "log_payload"),
        ((_result_row(),), cast("Any", 1), TypeError, "with_logs"),
    ],
)
def test_parse_result_rows_rejects_projection_drift(
    rows: tuple[tuple[object, ...], ...],
    with_logs: object,
    error_type: type[Exception],
    match: str,
) -> None:
    """Fail closed on duplicate, wrong-width, corrupt-state or decoded rows."""
    with pytest.raises(error_type, match=match):
        parse_result_rows(rows, point=RESULT_POINT, with_logs=with_logs)


def test_parse_result_rows_requires_a_validated_result_point() -> None:
    """Reject a raw scope even when ClickHouse returns no physical row."""
    with pytest.raises(TypeError, match="result point must be a ResultPoint"):
        parse_result_rows((), point=cast("Any", object()), with_logs=False)


def test_parse_progress_rows_and_visibility_are_strict() -> None:
    """Parse exact bytes and evaluate exclusive visibility in Python."""
    assert parse_progress_rows(()) is None
    visible = parse_progress_rows(((NOW, NOW, GENERATION_ID, VISIBLE, PURGE, b"progress"),))
    expired = ProgressRead(VISIBLE, NOW, GENERATION_ID, VISIBLE, PURGE, b"progress")

    assert visible is not None
    assert visible.is_visible is True
    assert expired.is_visible is False
    with pytest.raises(TypeError, match="progress_payload"):
        parse_progress_rows(((NOW, NOW, GENERATION_ID, VISIBLE, PURGE, "decoded"),))
    with pytest.raises(ValueError, match="at most one row"):
        parse_progress_rows(
            (
                (NOW, NOW, GENERATION_ID, VISIBLE, PURGE, b"one"),
                (NOW, NOW, OTHER_GENERATION_ID, VISIBLE, PURGE, b"two"),
            ),
        )


@pytest.mark.parametrize(
    ("rows", "expected"),
    [
        ((), False),
        (((1,),), True),
    ],
)
def test_parse_confirmation_rows_accepts_only_absence_or_literal_one(
    rows: tuple[tuple[object, ...], ...],
    expected: object,
) -> None:
    """Interpret an exact identity proof without truthiness coercion."""
    assert parse_confirmation_rows(rows) is expected


@pytest.mark.parametrize("rows", [((True,),), ((0,),), ((2,),), (("1",),)])
def test_parse_confirmation_rows_rejects_non_literal_one(rows: tuple[tuple[object, ...], ...]) -> None:
    """Reject booleans, other integers and decoded strings as corruption."""
    with pytest.raises(ValueError, match="literal 1"):
        parse_confirmation_rows(rows)


def test_build_tombstone_preserves_target_and_extends_retention_floor() -> None:
    """Target only the consumed generation and retain its suppression history."""
    selected = _selected_result(purge_at=NOW + timedelta(hours=2))
    tombstone = build_tombstone(selected, RETENTION)

    assert tombstone.state == TOMBSTONE_STATE
    assert (tombstone.namespace, tombstone.task_id) == (
        selected.point.namespace,
        selected.point.task_id,
    )
    assert tombstone.generation_at == selected.generation_at
    assert tombstone.generation_id == selected.generation_id
    assert tombstone.written_at == selected.observed_at
    assert tombstone.visible_until == selected.visible_until
    assert tombstone.purge_at == selected.observed_at + timedelta(days=1)
    assert tombstone.result_payload == tombstone.log_payload == b""


def test_build_tombstone_keeps_later_selected_purge_deadline() -> None:
    """Never make a tombstone eligible before the result it hides."""
    selected = _selected_result(purge_at=NOW + timedelta(days=2))

    tombstone = build_tombstone(selected, RETENTION)

    assert tombstone.purge_at == selected.purge_at


@pytest.mark.parametrize(
    "selected",
    [
        _selected_result(state=TOMBSTONE_STATE),
        _selected_result(observed_at=VISIBLE),
    ],
)
def test_build_tombstone_rejects_non_visible_result(selected: ResultRead) -> None:
    """Do not consume an expired row or an already-selected tombstone."""
    with pytest.raises(ValueError, match="visible result"):
        build_tombstone(selected, RETENTION)


def test_build_tombstone_rejects_bad_retention_and_overflow() -> None:
    """Validate finite tombstone retention before building a native row."""
    invalid_retention: Any = True
    with pytest.raises(TypeError, match="retention must be a RetentionPolicy"):
        build_tombstone(_selected_result(), invalid_retention)
    near_maximum = _selected_result(
        observed_at=DATETIME64_MAX - 2 * MICROSECOND,
        visible_until=DATETIME64_MAX - MICROSECOND,
        purge_at=DATETIME64_MAX,
    )
    with pytest.raises(ValueError, match="purge_at"):
        build_tombstone(near_maximum, RetentionPolicy(1, 3))
