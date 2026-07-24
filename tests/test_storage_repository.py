"""Validate the Taskiq-free storage repository and bounded write protocol."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, TypeAlias, cast
from uuid import UUID

import pytest

from taskiq_clickhouse._clickhouse.errors import (
    AmbiguousClickHouseError,
    DefiniteClickHouseError,
)
from taskiq_clickhouse._clickhouse.queries import UNCACHED_READ_SETTINGS
from taskiq_clickhouse._storage import (
    bindings as storage_bindings,
    generation as storage_generation,
    queries as storage_queries,
)
from taskiq_clickhouse._storage.acknowledged_writer import (
    STORAGE_WRITE_SETTINGS,
)
from taskiq_clickhouse._storage.layout import storage_layout_from_names
from taskiq_clickhouse._storage.repository import StorageRepository
from taskiq_clickhouse._storage.result_records import (
    RESULT_STATE,
    TOMBSTONE_STATE,
    ResultPoint,
    ResultRead,
)
from taskiq_clickhouse._storage_policy import NamespaceKey, RetentionPolicy, StoragePolicy
from taskiq_clickhouse.exceptions import (
    ClickHouseBackendIOError,
    ClickHouseDataCorruptionError,
    ClickHouseProgressError,
)


if TYPE_CHECKING:
    from taskiq_clickhouse._clickhouse.request import InsertRequest
    from taskiq_clickhouse._storage.layout import StorageLayout


Rows: TypeAlias = tuple[tuple[object, ...], ...]
QueryEvent: TypeAlias = Rows | BaseException
InsertEvent: TypeAlias = None | BaseException
QueryCall: TypeAlias = tuple[
    str,
    Mapping[str, object] | None,
    Mapping[str, object] | None,
    Mapping[str, str] | None,
]

NOW = datetime(2026, 7, 16, 9, 30, 0, 123456, tzinfo=UTC)
VISIBLE = NOW + timedelta(hours=1)
PURGE = NOW + timedelta(days=1)
GENERATION_ID = UUID("12345678-1234-4234-9234-123456789abc")
RESULT_TTL_US = 3_600_000_000
PURGE_TTL_US = 86_400_000_000
POINT_BINDINGS = {"namespace": "tenant:blue", "task_id": "task\x00id"}
MAX_WRITE_ATTEMPTS = 2
ALLOCATOR_AND_CONFIRMATION_QUERIES = 3
LAYOUT = storage_layout_from_names("tasks", "results", "progress")
POLICY = StoragePolicy(
    namespace=NamespaceKey("tenant:blue"),
    retention=RetentionPolicy(RESULT_TTL_US, PURGE_TTL_US),
)
RESULT_QUERIES = storage_queries.ResultQueries(LAYOUT.result_table)
PROGRESS_QUERIES = storage_queries.ProgressQueries(LAYOUT.progress_table)
RESULT_POINT_BINDINGS = RESULT_QUERIES.bind(POINT_BINDINGS)
PROGRESS_POINT_BINDINGS = PROGRESS_QUERIES.bind(POINT_BINDINGS)
RESULT_POINT = ResultPoint("tenant:blue", "task\x00id", LAYOUT.result_table)


@dataclass(slots=True)
class _ScriptedGateway:
    """Record exact repository I/O while returning deterministic events."""

    query_events: list[QueryEvent] = field(default_factory=list)
    insert_events: list[InsertEvent] = field(default_factory=list)
    queries: list[QueryCall] = field(default_factory=list)
    inserts: list[InsertRequest] = field(default_factory=list)

    async def query_rows(
        self,
        query: str,
        *,
        query_parameters: Mapping[str, object] | None = None,
        settings: Mapping[str, object] | None = None,
        column_formats: Mapping[str, str] | None = None,
    ) -> Rows:
        """Consume one query event and retain all repository-owned arguments."""
        assert settings is UNCACHED_READ_SETTINGS
        assert dict(settings) == {"use_query_cache": 0}
        self.queries.append((query, query_parameters, settings, column_formats))
        event = self.query_events.pop(0)
        if isinstance(event, BaseException):
            raise event
        return event

    async def insert_rows(self, request: InsertRequest) -> None:
        """Consume one native-insert event while preserving request identity."""
        self.inserts.append(request)
        event = self.insert_events.pop(0)
        if isinstance(event, BaseException):
            raise event


class _FatalStorageSignal(BaseException):
    """Terminal signal that no storage boundary may translate or retain."""


def _repository(
    gateway: _ScriptedGateway,
    *,
    uuid_factory: object = lambda: GENERATION_ID,
) -> StorageRepository:
    return StorageRepository(
        gateway=gateway,
        layout=LAYOUT,
        policy=POLICY,
        uuid_factory=cast("storage_generation.UUIDFactory", uuid_factory),
    )


def _result_row(
    *,
    state: int = RESULT_STATE,
    observed_at: datetime = NOW,
    visible_until: datetime = VISIBLE,
    with_logs: bool = False,
) -> tuple[object, ...]:
    payload = b"" if state == TOMBSTONE_STATE else b"result\x00\xff"
    log_payload = b"" if state == TOMBSTONE_STATE else b"log\x00\xff"
    row: tuple[object, ...] = (
        observed_at,
        NOW,
        GENERATION_ID,
        state,
        visible_until,
        PURGE,
        payload,
    )
    if with_logs:
        return (*row, log_payload)
    return row


def _selected_result() -> ResultRead:
    return ResultRead(
        point=RESULT_POINT,
        observed_at=NOW,
        generation_at=NOW,
        generation_id=GENERATION_ID,
        state=RESULT_STATE,
        visible_until=VISIBLE,
        purge_at=PURGE,
        result_payload=b"result",
        log_payload=b"log",
    )


@pytest.mark.parametrize(
    ("replacements", "match"),
    [
        pytest.param(
            {"layout": cast("StorageLayout", object())},
            "StorageLayout",
            id="unvalidated-layout",
        ),
        pytest.param(
            {"policy": cast("StoragePolicy", object())},
            "StoragePolicy",
            id="unvalidated-policy",
        ),
        pytest.param(
            {"uuid_factory": cast("object", 1)},
            "callable",
            id="non-callable-uuid-factory",
        ),
    ],
)
def test_repository_requires_validated_policy_and_layout_without_io(
    replacements: Mapping[str, object],
    match: str,
) -> None:
    """Reject raw collaborators instead of rebuilding policy invariants."""
    gateway = _ScriptedGateway()
    layout = storage_layout_from_names("tasks", "results", "progress")
    base = {
        "gateway": gateway,
        "layout": layout,
        "policy": POLICY,
    }

    with pytest.raises(TypeError, match=match):
        StorageRepository(**{**base, **replacements})  # type: ignore[arg-type]

    assert gateway.queries == []
    assert gateway.inserts == []


@pytest.mark.asyncio
async def test_result_write_freezes_full_native_request_and_allocation() -> None:
    """Use server time, all columns/types and exact synchronous settings."""
    gateway = _ScriptedGateway(query_events=[((NOW, None, None),)], insert_events=[None])
    repository = _repository(gateway)

    record = await repository.write_result("task\x00id", b"result\x00\xff", b"log\x00\xff")

    assert record.as_row() == gateway.inserts[0].rows[0]
    assert record.generation_at == record.written_at == NOW
    assert record.generation_id == GENERATION_ID
    assert record.visible_until == VISIBLE
    assert record.purge_at == PURGE
    assert gateway.queries == [
        (
            RESULT_QUERIES.allocator,
            RESULT_POINT_BINDINGS,
            UNCACHED_READ_SETTINGS,
            None,
        ),
    ]
    request = gateway.inserts[0]
    assert (request.database, request.table) == ("tasks", "results")
    assert request.column_names == storage_queries.RESULT_INSERT_COLUMN_NAMES
    assert request.column_type_names == storage_queries.RESULT_INSERT_COLUMN_TYPES
    assert request.settings == STORAGE_WRITE_SETTINGS
    assert dict(request.settings) == {
        "async_insert": 0,
        "wait_for_async_insert": 1,
        "wait_end_of_query": 1,
    }


@pytest.mark.asyncio
async def test_progress_write_uses_independent_allocator_and_contract() -> None:
    """Persist progress through its own table, projection and exact native tuple."""
    gateway = _ScriptedGateway(
        query_events=[((NOW, NOW - timedelta(seconds=1), PURGE),)],
        insert_events=[None],
    )
    repository = _repository(gateway)

    record = await repository.write_progress("task\x00id", b"progress\x00\xff")

    request = gateway.inserts[0]
    assert request.rows == (record.as_row(),)
    assert request.table == "progress"
    assert request.column_names == storage_queries.PROGRESS_INSERT_COLUMN_NAMES
    assert request.column_type_names == storage_queries.PROGRESS_INSERT_COLUMN_TYPES
    assert request.settings == STORAGE_WRITE_SETTINGS
    assert gateway.queries[0] == (
        PROGRESS_QUERIES.allocator,
        PROGRESS_POINT_BINDINGS,
        UNCACHED_READ_SETTINGS,
        None,
    )


@pytest.mark.asyncio
async def test_ambiguous_result_write_confirms_exact_identity() -> None:
    """Treat exact post-error presence as acknowledgement without a retry."""
    gateway = _ScriptedGateway(
        query_events=[((NOW, None, None),), ((1,),)],
        insert_events=[AmbiguousClickHouseError()],
    )
    repository = _repository(gateway)

    record = await repository.write_result("task\x00id", b"result", b"log")

    assert len(gateway.inserts) == 1
    assert gateway.queries[1] == (
        RESULT_QUERIES.confirmation,
        RESULT_QUERIES.bind(storage_bindings.result_confirmation_parameters(record)),
        UNCACHED_READ_SETTINGS,
        None,
    )


@pytest.mark.asyncio
async def test_ambiguous_result_write_retries_same_frozen_request_once() -> None:
    """Retry only after confirmed absence and reuse the identical request object."""
    gateway = _ScriptedGateway(
        query_events=[((NOW, None, None),), (), ((1,),)],
        insert_events=[AmbiguousClickHouseError(), AmbiguousClickHouseError()],
    )
    repository = _repository(gateway)

    record = await repository.write_result("task\x00id", b"result", b"log")

    assert len(gateway.inserts) == MAX_WRITE_ATTEMPTS
    assert gateway.inserts[0] is gateway.inserts[1]
    assert gateway.inserts[0].rows == (record.as_row(),)
    assert gateway.queries[1][1] == gateway.queries[2][1]


@pytest.mark.asyncio
async def test_retry_can_receive_direct_ack_without_final_confirmation() -> None:
    """Stop after the frozen retry itself receives an acknowledged response."""
    gateway = _ScriptedGateway(
        query_events=[((NOW, None, None),), ()],
        insert_events=[AmbiguousClickHouseError(), None],
    )

    await _repository(gateway).write_result("task\x00id", b"result", b"log")

    assert len(gateway.inserts) == MAX_WRITE_ATTEMPTS
    assert gateway.inserts[0] is gateway.inserts[1]
    assert len(gateway.queries) == MAX_WRITE_ATTEMPTS


@pytest.mark.asyncio
async def test_final_confirmed_absence_fails_after_two_frozen_attempts() -> None:
    """Bound response-loss handling to two attempts and two confirmations."""
    gateway = _ScriptedGateway(
        query_events=[((NOW, None, None),), (), ()],
        insert_events=[AmbiguousClickHouseError(), AmbiguousClickHouseError()],
    )

    with pytest.raises(ClickHouseBackendIOError, match="write_unconfirmed") as raised:
        await _repository(gateway).write_result("task\x00id", b"result", b"log")

    assert len(gateway.inserts) == MAX_WRITE_ATTEMPTS
    assert gateway.inserts[0] is gateway.inserts[1]
    assert len(gateway.queries) == ALLOCATOR_AND_CONFIRMATION_QUERIES
    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("confirmation_error", "reason"),
    [
        (AmbiguousClickHouseError(), "ambiguous_response"),
        (DefiniteClickHouseError(), "database_error"),
    ],
)
async def test_confirmation_failure_never_triggers_blind_retry(
    confirmation_error: BaseException,
    reason: str,
) -> None:
    """Propagate a classified confirmation failure after one insert attempt."""
    gateway = _ScriptedGateway(
        query_events=[((NOW, None, None),), confirmation_error],
        insert_events=[AmbiguousClickHouseError()],
    )

    with pytest.raises(ClickHouseBackendIOError, match=reason):
        await _repository(gateway).write_result("task\x00id", b"result", b"log")

    assert len(gateway.inserts) == 1
    assert len(gateway.queries) == MAX_WRITE_ATTEMPTS


@pytest.mark.asyncio
async def test_confirmation_cancellation_propagates_without_retry() -> None:
    """Preserve cancellation received by the exact-identity confirmation."""
    cancellation = asyncio.CancelledError()
    gateway = _ScriptedGateway(
        query_events=[((NOW, None, None),), cancellation],
        insert_events=[AmbiguousClickHouseError()],
    )

    with pytest.raises(asyncio.CancelledError) as raised:
        await _repository(gateway).write_result("task\x00id", b"result", b"log")

    assert raised.value is cancellation
    assert len(gateway.inserts) == 1
    assert len(gateway.queries) == MAX_WRITE_ATTEMPTS


@pytest.mark.asyncio
async def test_definite_insert_failure_is_not_confirmed_or_retried() -> None:
    """Translate a definite database rejection immediately and detach secrets."""
    gateway = _ScriptedGateway(
        query_events=[((NOW, None, None),)],
        insert_events=[DefiniteClickHouseError()],
    )

    with pytest.raises(ClickHouseBackendIOError, match="database_error") as raised:
        await _repository(gateway).write_result("task\x00id", b"secret", b"secret-log")

    assert len(gateway.inserts) == 1
    assert len(gateway.queries) == 1
    assert "secret" not in str(raised.value)
    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None


@pytest.mark.asyncio
async def test_write_cancellation_propagates_without_confirmation_or_retry() -> None:
    """Preserve cancellation identity after allocation and one insert attempt."""
    cancellation = asyncio.CancelledError()
    gateway = _ScriptedGateway(
        query_events=[((NOW, None, None),)],
        insert_events=[cancellation],
    )

    with pytest.raises(asyncio.CancelledError) as raised:
        await _repository(gateway).write_result("task\x00id", b"result", b"log")

    assert raised.value is cancellation
    assert len(gateway.inserts) == 1
    assert len(gateway.queries) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["allocation", "insert", "confirmation"])
async def test_fatal_write_signal_preserves_exact_identity(stage: str) -> None:
    """Never subordinate a terminal signal at any shared write-protocol stage."""
    fatal = _FatalStorageSignal()
    query_events: list[QueryEvent] = [((NOW, None, None),)]
    insert_events: list[InsertEvent] = [None]
    if stage == "allocation":
        query_events = [fatal]
        insert_events = []
    elif stage == "insert":
        insert_events = [fatal]
    else:
        query_events.append(fatal)
        insert_events = [AmbiguousClickHouseError()]
    gateway = _ScriptedGateway(query_events=query_events, insert_events=insert_events)

    with pytest.raises(_FatalStorageSignal) as raised:
        await _repository(gateway).write_result("task\x00id", b"result", b"log")

    assert raised.value is fatal


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("row", "expected"),
    [
        (None, False),
        ((NOW, NOW, GENERATION_ID, RESULT_STATE, VISIBLE, PURGE), True),
        ((VISIBLE, NOW, GENERATION_ID, RESULT_STATE, VISIBLE, PURGE), False),
        ((NOW, NOW, GENERATION_ID, TOMBSTONE_STATE, VISIBLE, PURGE), False),
    ],
)
async def test_readiness_filters_latest_state_only_after_parsing(
    row: tuple[object, ...] | None,
    *,
    expected: bool,
) -> None:
    """Return false for missing, equality-expired and tombstone latest rows."""
    rows = () if row is None else (row,)
    gateway = _ScriptedGateway(query_events=[rows])
    repository = _repository(gateway)

    assert await repository.is_result_ready("task\x00id") is expected
    query, bindings, settings, formats = gateway.queries[0]
    assert query == RESULT_QUERIES.readiness
    assert bindings == RESULT_POINT_BINDINGS
    assert settings is UNCACHED_READ_SETTINGS
    assert formats is None
    assert "payload" not in query


@pytest.mark.asyncio
async def test_readiness_rejects_corrupt_deadlines_without_reading_payloads() -> None:
    """Translate invalid latest-row retention into a safe corruption error."""
    corrupt_row = (NOW, NOW, GENERATION_ID, RESULT_STATE, VISIBLE, VISIBLE)
    gateway = _ScriptedGateway(query_events=[(corrupt_row,)])

    with pytest.raises(ClickHouseDataCorruptionError, match="projection_corrupt") as raised:
        await _repository(gateway).is_result_ready("task\x00id")

    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None
    query = gateway.queries[0][0]
    assert "purge_at" in query
    assert "payload" not in query


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("row", "expected"),
    [
        (None, None),
        (_result_row(), b"result\x00\xff"),
        (_result_row(observed_at=VISIBLE, visible_until=VISIBLE), None),
        (_result_row(state=TOMBSTONE_STATE), None),
    ],
)
async def test_no_log_read_returns_only_visible_latest_result(
    row: tuple[object, ...] | None,
    expected: bytes | None,
) -> None:
    """Omit logs physically and hide missing, expired or consumed latest rows."""
    gateway = _ScriptedGateway(query_events=[() if row is None else (row,)])
    repository = _repository(gateway)

    selected = await repository.read_result_no_log("task\x00id")

    assert (None if selected is None else selected.result_payload) == expected
    assert selected is None or selected.log_payload is None
    assert selected is None or selected.point == RESULT_POINT
    query, bindings, settings, formats = gateway.queries[0]
    assert query == RESULT_QUERIES.no_log
    assert bindings == RESULT_POINT_BINDINGS
    assert settings is UNCACHED_READ_SETTINGS
    assert formats is storage_queries.NO_LOG_COLUMN_FORMATS
    assert "log_payload" not in query


@pytest.mark.asyncio
async def test_non_empty_tombstone_payload_is_result_corruption() -> None:
    """Reject a malformed tombstone instead of treating retained bytes as absence."""
    malformed = (
        NOW,
        NOW,
        GENERATION_ID,
        TOMBSTONE_STATE,
        VISIBLE,
        PURGE,
        b"must-have-been-empty",
    )
    gateway = _ScriptedGateway(query_events=[(malformed,)])

    with pytest.raises(ClickHouseDataCorruptionError, match="projection_corrupt") as raised:
        await _repository(gateway).read_result_no_log("task\x00id")

    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None


@pytest.mark.asyncio
async def test_with_log_read_uses_one_row_and_both_explicit_byte_formats() -> None:
    """Return result/log bytes from one exact latest-row projection."""
    gateway = _ScriptedGateway(query_events=[(_result_row(with_logs=True),)])
    repository = _repository(gateway)

    selected = await repository.read_result_with_log("task\x00id")

    assert selected is not None
    assert selected.point == RESULT_POINT
    assert (selected.result_payload, selected.log_payload) == (b"result\x00\xff", b"log\x00\xff")
    query, bindings, settings, formats = gateway.queries[0]
    assert query == RESULT_QUERIES.with_log
    assert bindings == RESULT_POINT_BINDINGS
    assert settings is UNCACHED_READ_SETTINGS
    assert formats is storage_queries.WITH_LOG_COLUMN_FORMATS


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("row", "expected"),
    [
        (None, None),
        ((NOW, NOW, GENERATION_ID, VISIBLE, PURGE, b"progress\xff"), b"progress\xff"),
        ((VISIBLE, NOW, GENERATION_ID, VISIBLE, PURGE, b"expired"), None),
    ],
)
async def test_progress_read_returns_only_visible_latest_update(
    row: tuple[object, ...] | None,
    expected: bytes | None,
) -> None:
    """Evaluate progress visibility in Python using same-query server time."""
    gateway = _ScriptedGateway(query_events=[() if row is None else (row,)])
    repository = _repository(gateway)

    selected = await repository.read_progress("task\x00id")

    assert (None if selected is None else selected.progress_payload) == expected
    query, bindings, settings, formats = gateway.queries[0]
    assert query == PROGRESS_QUERIES.latest
    assert bindings == PROGRESS_POINT_BINDINGS
    assert settings is UNCACHED_READ_SETTINGS
    assert formats is storage_queries.PROGRESS_COLUMN_FORMATS


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method_name", "rows", "error_type"),
    [
        ("is_result_ready", ((NOW,),), ClickHouseDataCorruptionError),
        ("read_result_no_log", ((NOW,),), ClickHouseDataCorruptionError),
        ("read_result_with_log", ((NOW,),), ClickHouseDataCorruptionError),
        ("read_progress", ((NOW,),), ClickHouseProgressError),
    ],
)
async def test_malformed_read_projection_uses_safe_domain_error(
    method_name: str,
    rows: Rows,
    error_type: type[Exception],
) -> None:
    """Translate persisted row drift without retaining raw parser context."""
    gateway = _ScriptedGateway(query_events=[rows])
    method = getattr(_repository(gateway), method_name)

    with pytest.raises(error_type, match="projection_corrupt") as raised:
        await method("task\x00id")

    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("write_kind", "allocator_rows", "error_type"),
    [
        ("result", (), ClickHouseDataCorruptionError),
        ("progress", (("not-a-time", None, None),), ClickHouseProgressError),
    ],
)
async def test_malformed_allocator_projection_fails_before_insert(
    write_kind: str,
    allocator_rows: Rows,
    error_type: type[Exception],
) -> None:
    """Treat malformed server allocation state as result/progress corruption."""
    gateway = _ScriptedGateway(query_events=[allocator_rows])
    repository = _repository(gateway)
    write = (
        repository.write_result("task\x00id", b"result", b"log")
        if write_kind == "result"
        else repository.write_progress("task\x00id", b"progress")
    )

    with pytest.raises(error_type, match="projection_corrupt") as raised:
        await write

    assert gateway.inserts == []
    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None


@pytest.mark.asyncio
async def test_allocation_overflow_is_a_safe_corruption_failure() -> None:
    """Reject an exhausted persisted generation timestamp before native insert."""
    maximum = datetime(2299, 12, 31, 23, 59, 59, 999999, tzinfo=UTC)
    gateway = _ScriptedGateway(query_events=[((NOW, maximum, maximum),)])

    with pytest.raises(ClickHouseDataCorruptionError, match="allocation_invalid") as raised:
        await _repository(gateway).write_result("task\x00id", b"result", b"log")

    assert gateway.inserts == []
    assert raised.value.__context__ is None


@pytest.mark.asyncio
async def test_tombstone_targets_selected_generation_with_empty_payloads() -> None:
    """Insert no new generation and extend selected retention before acknowledgement."""
    selected = _selected_result()
    gateway = _ScriptedGateway(insert_events=[None])
    repository = _repository(gateway)

    record = await repository.write_tombstone(selected)

    assert gateway.queries == []
    assert record.state == TOMBSTONE_STATE
    assert (record.namespace, record.task_id) == (
        selected.point.namespace,
        selected.point.task_id,
    )
    assert (record.generation_at, record.generation_id) == (
        selected.generation_at,
        selected.generation_id,
    )
    assert record.written_at == selected.observed_at
    assert record.visible_until == selected.visible_until
    assert record.purge_at == selected.observed_at + timedelta(days=1)
    assert record.result_payload == record.log_payload == b""
    assert gateway.inserts[0].rows == (record.as_row(),)


@pytest.mark.asyncio
async def test_tombstone_deadline_overflow_is_a_safe_corruption_failure() -> None:
    """Translate an unrepresentable consume deadline before native insert."""
    maximum = datetime(2299, 12, 31, 23, 59, 59, 999999, tzinfo=UTC)
    selected = replace(
        _selected_result(),
        observed_at=maximum - 2 * timedelta(microseconds=1),
        visible_until=maximum - timedelta(microseconds=1),
        purge_at=maximum,
    )
    gateway = _ScriptedGateway()

    with pytest.raises(ClickHouseDataCorruptionError, match="tombstone_invalid") as raised:
        await _repository(gateway).write_tombstone(selected)

    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None
    assert gateway.queries == []
    assert gateway.inserts == []


@pytest.mark.asyncio
async def test_tombstone_rejects_a_no_longer_visible_selection() -> None:
    """Never acknowledge a stale selection after its logical availability ended."""
    selected = _selected_result()
    expired = replace(selected, observed_at=selected.visible_until)
    gateway = _ScriptedGateway()

    with pytest.raises(ClickHouseDataCorruptionError, match="tombstone_invalid") as raised:
        await _repository(gateway).write_tombstone(expired)

    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None
    assert gateway.queries == []
    assert gateway.inserts == []


@pytest.mark.asyncio
async def test_tombstone_ambiguous_write_confirms_stateful_identity() -> None:
    """Include tombstone state in confirmation so a result row cannot confirm it."""
    gateway = _ScriptedGateway(
        query_events=[((1,),)],
        insert_events=[AmbiguousClickHouseError()],
    )
    repository = _repository(gateway)

    record = await repository.write_tombstone(_selected_result())

    query, bindings, settings, formats = gateway.queries[0]
    assert query == RESULT_QUERIES.confirmation
    assert bindings == RESULT_QUERIES.bind(storage_bindings.result_confirmation_parameters(record))
    assert bindings["state"] == TOMBSTONE_STATE
    assert settings is UNCACHED_READ_SETTINGS
    assert formats is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "point",
    [
        ResultPoint("other", RESULT_POINT.task_id, RESULT_POINT.result_table),
        ResultPoint(
            RESULT_POINT.namespace,
            RESULT_POINT.task_id,
            storage_layout_from_names("tasks", "other_results", "progress").result_table,
        ),
        ResultPoint(
            RESULT_POINT.namespace,
            RESULT_POINT.task_id,
            storage_layout_from_names("other", "results", "progress").result_table,
        ),
    ],
)
async def test_tombstone_rejects_a_selection_from_another_repository_scope(
    point: ResultPoint,
) -> None:
    """Reject cross-namespace and cross-table capabilities before storage I/O."""
    gateway = _ScriptedGateway()
    selected = replace(_selected_result(), point=point)

    with pytest.raises(ValueError, match="repository scope"):
        await _repository(gateway).write_tombstone(selected)

    assert gateway.queries == []
    assert gateway.inserts == []


@pytest.mark.asyncio
async def test_tombstone_rejects_a_non_selection_before_storage_io() -> None:
    """Require a validated ResultRead value instead of an unrelated object."""
    gateway = _ScriptedGateway()

    with pytest.raises(TypeError, match="ResultRead"):
        await _repository(gateway).write_tombstone(cast("ResultRead", object()))

    assert gateway.queries == []
    assert gateway.inserts == []


@pytest.mark.asyncio
async def test_progress_confirmation_corruption_uses_progress_error() -> None:
    """Keep malformed progress acknowledgement in the progress taxonomy."""
    gateway = _ScriptedGateway(
        query_events=[((NOW, None, None),), ((2,),)],
        insert_events=[AmbiguousClickHouseError()],
    )

    with pytest.raises(ClickHouseProgressError, match="projection_corrupt") as raised:
        await _repository(gateway).write_progress("task\x00id", b"progress")

    assert len(gateway.inserts) == 1
    assert raised.value.__context__ is None


@pytest.mark.asyncio
async def test_ambiguous_progress_write_confirms_complete_identity() -> None:
    """Confirm progress with its point key, timestamp and UUID tie-breaker."""
    gateway = _ScriptedGateway(
        query_events=[((NOW, None, None),), ((1,),)],
        insert_events=[AmbiguousClickHouseError()],
    )
    repository = _repository(gateway)

    record = await repository.write_progress("task\x00id", b"progress")

    query, bindings, settings, formats = gateway.queries[1]
    assert query == PROGRESS_QUERIES.confirmation
    assert bindings == PROGRESS_QUERIES.bind(storage_bindings.progress_confirmation_parameters(record))
    assert settings is UNCACHED_READ_SETTINGS
    assert formats is None


@pytest.mark.asyncio
async def test_read_driver_failure_is_not_misreported_as_absence() -> None:
    """Preserve backend I/O failure instead of returning false or none."""
    gateway = _ScriptedGateway(query_events=[DefiniteClickHouseError()])

    with pytest.raises(ClickHouseBackendIOError, match="database_error"):
        await _repository(gateway).read_result_no_log("task\x00id")


def test_write_settings_are_exact_immutable_and_not_user_configurable() -> None:
    """Expose no arbitrary setting input and prevent in-process weakening."""
    assert dict(STORAGE_WRITE_SETTINGS) == {
        "async_insert": 0,
        "wait_for_async_insert": 1,
        "wait_end_of_query": 1,
    }
    with pytest.raises(TypeError, match="does not support item assignment"):
        cast("dict[str, int]", STORAGE_WRITE_SETTINGS)["wait_for_async_insert"] = 0


def test_read_settings_disable_cache_and_are_immutable() -> None:
    """Prevent stale storage observations and in-process cache re-enablement."""
    assert dict(UNCACHED_READ_SETTINGS) == {"use_query_cache": 0}
    with pytest.raises(TypeError, match="does not support item assignment"):
        cast("dict[str, int]", UNCACHED_READ_SETTINGS)["use_query_cache"] = 1
