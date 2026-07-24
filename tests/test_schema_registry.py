"""Unit tests for metadata layout, integrity and acknowledgement."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta, timezone
from typing import TYPE_CHECKING, cast
from uuid import UUID

import pytest

from taskiq_clickhouse._clickhouse.errors import (
    AmbiguousClickHouseError,
    DefiniteClickHouseError,
)
from taskiq_clickhouse._clickhouse.queries import (
    UNCACHED_READ_SETTINGS,
    QueryRequest,
    query_rows,
)
from taskiq_clickhouse._identifiers import Identifier
from taskiq_clickhouse._schema.codec import parse_records, parse_server_time
from taskiq_clickhouse._schema.integrity import (
    validate_history,
    validate_namespace_records,
)
from taskiq_clickhouse._schema.layout import (
    DDL_SETTINGS,
    METADATA_COLUMN_NAMES,
    METADATA_COLUMN_TYPES,
    METADATA_WRITE_SETTINGS,
    MetadataLayout,
)
from taskiq_clickhouse._schema.records import MetadataRecord
from taskiq_clickhouse._schema.registry import MetadataRegistry
from taskiq_clickhouse._schema.transport import ExactMetadataWriter
from taskiq_clickhouse._types import MigrationExecution
from taskiq_clickhouse.exceptions import (
    ClickHouseBackendIOError,
    ClickHouseMigrationError,
    ClickHouseNamespaceError,
)
from tests.factories.schema import MetadataRecordFactory
from tests.schema_testkit import (
    RECORDED_AT,
    ScriptedGateway,
    inserted_native_row,
    namespace_contract,
    native_row,
    synthetic_plan,
)


if TYPE_CHECKING:
    from taskiq_clickhouse._schema.contracts import SchemaContract
    from tests.schema_testkit import OperationEvent, QueryEvent


_RecordFactory = Callable[[MetadataRecord], tuple[MetadataRecord, ...]]


@dataclass(slots=True)
class _Verifier:
    matches_result: bool = True
    validated: list[SchemaContract] = field(default_factory=list)
    matched: list[SchemaContract] = field(default_factory=list)

    async def matches(self, contract: SchemaContract) -> bool:
        self.matched.append(contract)
        return self.matches_result

    async def validate(self, contract: SchemaContract) -> None:
        self.validated.append(contract)


def test_metadata_layout_is_exact_and_non_configurable() -> None:
    """Freeze DDL, physical contract, settings and explicit column order."""
    layout = MetadataLayout(Identifier("test_db"))

    assert layout.table.canonical == "test_db.taskiq_clickhouse_metadata"
    assert layout.contract.tables[0].critical_settings == ()
    assert layout.contract.tables[0].ttl_expression == ""
    assert layout.contract.tables[0].allowed_additive_columns == ()
    assert tuple(column.name.value for column in layout.contract.tables[0].columns) == METADATA_COLUMN_NAMES
    assert tuple(column.type_name for column in layout.contract.tables[0].columns) == METADATA_COLUMN_TYPES
    assert "CREATE TABLE IF NOT EXISTS {database:Identifier}.{table:Identifier}" in layout.create_query
    assert layout.table_parameters == {
        "database": "test_db",
        "table": "taskiq_clickhouse_metadata",
    }
    assert "PARTITION BY" not in layout.create_query
    assert "TTL" not in layout.create_query
    assert "attempt_id" in layout.read_query
    assert "attempt_id = {attempt_id:UUID}" in layout.confirmation_query
    assert "{database:Identifier}.{table:Identifier}" in layout.read_query
    assert "{database:Identifier}.{table:Identifier}" in layout.confirmation_query
    assert dict(DDL_SETTINGS) == {"wait_end_of_query": 1}
    assert METADATA_WRITE_SETTINGS["async_insert"] == 0


def test_metadata_codec_requires_bytes_and_canonical_payload() -> None:
    """Reject decoded strings and malformed physical row widths."""
    record = MetadataRecordFactory.from_migration(synthetic_plan().migrations[0])

    assert parse_records((native_row(record),), operation="migration_history") == (record,)
    broken_row = list(native_row(record))
    broken_row[0] = "migration"

    with pytest.raises(ClickHouseMigrationError, match="record_corrupt"):
        parse_records((tuple(broken_row),), operation="migration_history")
    with pytest.raises(ClickHouseMigrationError, match="record_corrupt"):
        parse_records(((b"too", b"short"),), operation="migration_history")


@pytest.mark.parametrize(
    ("index", "invalid_value"),
    [
        pytest.param(3, True, id="version-rejects-bool"),
        pytest.param(5, "not-bytes", id="payload-rejects-text"),
    ],
)
def test_metadata_codec_rejects_invalid_physical_cell(
    index: int,
    invalid_value: object,
) -> None:
    """Report every invalid physical cell as an independent test case."""
    record = MetadataRecordFactory.from_migration(synthetic_plan().migrations[0])
    invalid_row = list(native_row(record))
    invalid_row[index] = invalid_value

    with pytest.raises(ClickHouseMigrationError, match="record_corrupt"):
        parse_records((tuple(invalid_row),), operation="migration_history")


@pytest.mark.parametrize(
    "rows",
    [(), ((RECORDED_AT, RECORDED_AT),), (("not-a-datetime",),)],
)
def test_server_time_parser_rejects_every_wrong_shape(rows: tuple[tuple[object, ...], ...]) -> None:
    """Reject absent, multi-column and wrong-type server clock rows."""
    with pytest.raises(ClickHouseMigrationError, match="clock_invalid"):
        parse_server_time(rows)


def test_server_time_parser_accepts_one_datetime() -> None:
    """Accept the one exact server-clock projection shape."""
    assert parse_server_time(((RECORDED_AT,),)) is RECORDED_AT


@pytest.mark.parametrize(
    "recorded_at",
    [
        RECORDED_AT.replace(tzinfo=None),
        RECORDED_AT.astimezone(timezone(timedelta(hours=1))),
    ],
)
def test_server_time_parser_requires_aware_utc(recorded_at: object) -> None:
    """Translate invalid driver timezone values into the safe clock code."""
    with pytest.raises(ClickHouseMigrationError, match="clock_invalid"):
        parse_server_time(((recorded_at,),))


def test_history_accepts_exact_duplicates() -> None:
    """Treat identical concurrent evidence as one logical version."""
    plan = synthetic_plan()
    first = MetadataRecordFactory.from_migration(plan.migrations[0])
    second = replace(first, attempt_id=UUID("22345678-1234-4234-9234-123456789abc"))

    history = validate_history((first, second), plan, expected_scope=namespace_contract().scope)

    assert history.applied_version == 1
    assert history.records == (first, second)


def test_history_accepts_an_empty_prefix() -> None:
    """Represent an unstarted plan as applied version zero."""
    history = validate_history((), synthetic_plan(), expected_scope=namespace_contract().scope)

    assert history.applied_version == 0


@pytest.mark.parametrize(
    ("records_factory", "reason"),
    [
        (lambda record: (replace(record, version=2),), "version_gap"),
        (lambda record: (record, replace(record, version=2)), "newer_version"),
        (lambda record: (replace(record, scope="other.results|other.progress"),), "scope_mismatch"),
    ],
)
def test_history_rejects_invalid_version_or_scope(
    records_factory: _RecordFactory,
    reason: str,
) -> None:
    """Reject gaps, newer package history and poisoned scope rows."""
    plan = synthetic_plan()
    record = MetadataRecordFactory.from_migration(plan.migrations[0])
    records = records_factory(record)

    with pytest.raises(ClickHouseMigrationError, match=reason):
        validate_history(records, plan, expected_scope=namespace_contract().scope)


def test_history_rejects_conflicting_or_changed_definition() -> None:
    """Detect both concurrent definitions and immutable-code drift."""
    expected_plan = synthetic_plan()
    different_plan = synthetic_plan(execution=MigrationExecution.CONTROLLED)
    expected = MetadataRecordFactory.from_migration(expected_plan.migrations[0])
    different = MetadataRecordFactory.from_migration(different_plan.migrations[0])

    with pytest.raises(ClickHouseMigrationError, match="definition_conflict"):
        validate_history((expected, different), expected_plan, expected_scope=namespace_contract().scope)
    with pytest.raises(ClickHouseMigrationError, match="definition_changed"):
        validate_history((different,), expected_plan, expected_scope=namespace_contract().scope)


def test_namespace_integrity_accepts_duplicates_and_rejects_absence_or_conflict() -> None:
    """Allow exact duplicates while failing on absence or any conflict."""
    contract = namespace_contract()
    exact = MetadataRecordFactory.from_namespace(contract)
    duplicate = replace(exact, attempt_id=UUID("32345678-1234-4234-9234-123456789abc"))
    conflicting_contract = replace(contract, serializer_id="custom-json-v2")
    conflict = MetadataRecordFactory.from_namespace(conflicting_contract)

    validate_namespace_records((exact, duplicate), contract)
    with pytest.raises(ClickHouseNamespaceError, match="contract_missing"):
        validate_namespace_records((), contract)
    with pytest.raises(ClickHouseNamespaceError, match="contract_conflict"):
        validate_namespace_records((exact, conflict), contract)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("error_type", "reason"),
    [
        pytest.param(AmbiguousClickHouseError, "ambiguous_response", id="ambiguous-read"),
        pytest.param(DefiniteClickHouseError, "database_error", id="definite-read"),
    ],
)
async def test_safe_query_translates_adapter_failures_without_context(
    error_type: type[Exception],
    reason: str,
) -> None:
    """Map classified adapter reads to secret-free package failures."""
    gateway = ScriptedGateway(query_events=[error_type()])

    with pytest.raises(ClickHouseBackendIOError, match=reason) as raised:
        await query_rows(
            gateway,
            QueryRequest("SELECT 1", operation="metadata_read"),
        )

    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None


@pytest.mark.asyncio
async def test_safe_query_propagates_cancellation_identity() -> None:
    """Never wrap or retry caller cancellation."""
    cancellation = asyncio.CancelledError()
    gateway = ScriptedGateway(query_events=[cancellation])

    with pytest.raises(asyncio.CancelledError) as raised:
        await query_rows(
            gateway,
            QueryRequest("SELECT 1", operation="metadata_read"),
        )

    assert raised.value is cancellation


@pytest.mark.asyncio
async def test_safe_query_freezes_options_and_cannot_reenable_cache() -> None:
    """Keep every safe read fresh even when caller options request caching."""
    caller_settings = {"max_threads": 2, "use_query_cache": 1}
    gateway = ScriptedGateway(query_events=[((1,),), ((2,),)])

    first = await query_rows(
        gateway,
        QueryRequest("SELECT 1", operation="metadata_read"),
    )
    second = await query_rows(
        gateway,
        QueryRequest(
            "SELECT 2",
            operation="metadata_read",
            settings=caller_settings,
        ),
    )
    caller_settings["max_threads"] = 3

    assert (first, second) == (((1,),), ((2,),))
    assert gateway.query_settings[0] is UNCACHED_READ_SETTINGS
    assert dict(gateway.query_settings[1] or {}) == {
        "max_threads": 2,
        "use_query_cache": 0,
    }
    with pytest.raises(TypeError, match="does not support item assignment"):
        cast("dict[str, object]", gateway.query_settings[1])["use_query_cache"] = 1


@pytest.mark.asyncio
async def test_exact_writer_acknowledges_success_without_confirmation() -> None:
    """Avoid confirmation traffic after an acknowledged synchronous insert."""
    record = MetadataRecordFactory.from_migration(synthetic_plan().migrations[0])
    gateway = ScriptedGateway(insert_events=[None])

    await ExactMetadataWriter(gateway, MetadataLayout(Identifier("test_db"))).write(
        record,
        operation="migration_record_write",
    )

    assert len(gateway.inserts) == 1
    assert gateway.queries == []
    assert gateway.inserts[0].rows == (record.as_row(),)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("insert_events", "query_events", "expected_inserts"),
    [
        ([AmbiguousClickHouseError()], [inserted_native_row], 1),
        ([AmbiguousClickHouseError(), None], [()], 2),
        (
            [AmbiguousClickHouseError(), AmbiguousClickHouseError()],
            [(), inserted_native_row],
            2,
        ),
    ],
)
async def test_exact_writer_bounded_confirmation_success_branches(
    insert_events: list[OperationEvent],
    query_events: list[QueryEvent],
    expected_inserts: int,
) -> None:
    """Cover present, absent-retry and second-ambiguity confirmation paths."""
    record = _record()
    gateway = ScriptedGateway(
        insert_events=insert_events,
        query_events=query_events,
    )

    await ExactMetadataWriter(gateway, MetadataLayout(Identifier("test_db"))).write(
        record,
        operation="migration_record_write",
    )

    assert len(gateway.inserts) == expected_inserts
    assert all(request.rows == (record.as_row(),) for request in gateway.inserts)


@pytest.mark.asyncio
async def test_exact_writer_fails_after_bounded_final_absence() -> None:
    """Stop after one retry and a final absent confirmation."""
    gateway = ScriptedGateway(
        insert_events=[AmbiguousClickHouseError(), AmbiguousClickHouseError()],
        query_events=[(), ()],
    )

    with pytest.raises(ClickHouseBackendIOError, match="write_unconfirmed"):
        await ExactMetadataWriter(gateway, MetadataLayout(Identifier("test_db"))).write(
            _record(),
            operation="migration_record_write",
        )

    expected_attempt_count = 2
    assert len(gateway.inserts) == expected_attempt_count
    assert len(gateway.queries) == expected_attempt_count


@pytest.mark.asyncio
async def test_exact_writer_rejects_confirmation_conflict() -> None:
    """Fail when a frozen attempt identity maps to different row contents."""
    record = _record()
    conflict = replace(record, package_version="0.2.0")
    gateway = ScriptedGateway(
        insert_events=[AmbiguousClickHouseError()],
        query_events=[(native_row(conflict),)],
    )

    with pytest.raises(ClickHouseMigrationError, match="confirmation_conflict"):
        await ExactMetadataWriter(gateway, MetadataLayout(Identifier("test_db"))).write(
            record,
            operation="migration_record_write",
        )


@pytest.mark.asyncio
async def test_exact_writer_rejects_definite_insert_without_confirmation() -> None:
    """Translate a definite insert failure without confirmation or retry."""
    gateway = ScriptedGateway(insert_events=[DefiniteClickHouseError()])

    with pytest.raises(ClickHouseBackendIOError, match="database_error"):
        await ExactMetadataWriter(gateway, MetadataLayout(Identifier("test_db"))).write(
            _record(),
            operation="migration_record_write",
        )

    assert gateway.queries == []
    assert len(gateway.inserts) == 1


@pytest.mark.asyncio
async def test_exact_writer_propagates_insert_cancellation_without_confirmation() -> None:
    """Preserve cancellation identity without confirmation or retry."""
    cancellation = asyncio.CancelledError()
    gateway = ScriptedGateway(insert_events=[cancellation])

    with pytest.raises(asyncio.CancelledError) as raised:
        await ExactMetadataWriter(gateway, MetadataLayout(Identifier("test_db"))).write(
            _record(),
            operation="migration_record_write",
        )

    assert raised.value is cancellation
    assert gateway.queries == []
    assert len(gateway.inserts) == 1


@pytest.mark.asyncio
async def test_exact_writer_does_not_retry_ambiguous_confirmation() -> None:
    """Fail safely when the confirmation query itself is ambiguous."""
    gateway = ScriptedGateway(
        insert_events=[AmbiguousClickHouseError()],
        query_events=[AmbiguousClickHouseError()],
    )

    with pytest.raises(ClickHouseBackendIOError, match="ambiguous_response"):
        await ExactMetadataWriter(gateway, MetadataLayout(Identifier("test_db"))).write(
            _record(),
            operation="migration_record_write",
        )

    assert len(gateway.inserts) == 1


@pytest.mark.asyncio
async def test_registry_bootstrap_validate_is_write_free() -> None:
    """Prove validate mode invokes no command or insert."""
    gateway = ScriptedGateway()
    verifier = _Verifier()
    registry = MetadataRegistry(gateway, namespace_contract(), "0.1.0")

    await registry.bootstrap(verifier, mode="validate")

    assert gateway.commands == []
    assert gateway.inserts == []
    assert verifier.validated == [registry.layout.contract]


@pytest.mark.asyncio
async def test_registry_bootstrap_handles_acknowledged_and_confirmed_ddl() -> None:
    """Validate after both acknowledged and postcondition-confirmed bootstrap."""
    acknowledged_gateway = ScriptedGateway(command_events=[None])
    acknowledged_verifier = _Verifier()
    acknowledged = MetadataRegistry(acknowledged_gateway, namespace_contract(), "0.1.0")
    await acknowledged.bootstrap(acknowledged_verifier, mode="migrate")

    ambiguous_gateway = ScriptedGateway(command_events=[AmbiguousClickHouseError()])
    ambiguous_verifier = _Verifier(matches_result=True)
    ambiguous = MetadataRegistry(ambiguous_gateway, namespace_contract(), "0.1.0")
    await ambiguous.bootstrap(ambiguous_verifier, mode="migrate")

    assert acknowledged_gateway.commands == [
        (
            acknowledged.layout.create_query,
            acknowledged.layout.table_parameters,
        ),
    ]
    assert acknowledged_verifier.matched == []
    assert ambiguous_verifier.matched == [ambiguous.layout.contract]
    assert ambiguous_verifier.validated == [ambiguous.layout.contract]


@pytest.mark.asyncio
async def test_registry_bootstrap_rejects_unconfirmed_or_definite_ddl() -> None:
    """Fail closed for absent ambiguous postconditions and definite errors."""
    ambiguous = MetadataRegistry(
        ScriptedGateway(command_events=[AmbiguousClickHouseError()]),
        namespace_contract(),
        "0.1.0",
    )
    with pytest.raises(ClickHouseBackendIOError, match="ddl_unconfirmed"):
        await ambiguous.bootstrap(_Verifier(matches_result=False), mode="migrate")

    definite = MetadataRegistry(
        ScriptedGateway(command_events=[DefiniteClickHouseError()]),
        namespace_contract(),
        "0.1.0",
    )
    with pytest.raises(ClickHouseBackendIOError, match="database_error"):
        await definite.bootstrap(_Verifier(), mode="migrate")


def test_registry_rejects_empty_package_version() -> None:
    """Reject invalid local registry construction before I/O."""
    with pytest.raises(ValueError, match="package_version"):
        MetadataRegistry(ScriptedGateway(), namespace_contract(), "")


@pytest.mark.asyncio
async def test_registry_reads_history_and_records_migration_with_exact_columns() -> None:
    """Use complete typed columns for history reads and writes."""
    plan = synthetic_plan()
    existing = MetadataRecordFactory.from_migration(plan.migrations[0])
    read_gateway = ScriptedGateway(query_events=[(native_row(existing),)])
    registry = MetadataRegistry(read_gateway, namespace_contract(), "0.1.0")

    history = await registry.read_history(plan)

    assert history.applied_version == 1
    write_gateway = ScriptedGateway(query_events=[((RECORDED_AT,),)], insert_events=[None])
    write_registry = MetadataRegistry(write_gateway, namespace_contract(), "0.1.0")
    await write_registry.record_migration(plan.migrations[0])
    request = write_gateway.inserts[0]
    assert tuple(request.column_names) == METADATA_COLUMN_NAMES
    assert tuple(request.column_type_names) == METADATA_COLUMN_TYPES
    assert request.settings == METADATA_WRITE_SETTINGS


@pytest.mark.asyncio
async def test_registry_identical_namespace_does_not_grow() -> None:
    """Read-first startup leaves an identical namespace row count stable."""
    contract = namespace_contract()
    existing = MetadataRecordFactory.from_namespace(contract)
    gateway = ScriptedGateway(
        query_events=[((RECORDED_AT,),), (native_row(existing),)],
    )
    registry = MetadataRegistry(gateway, contract, "0.1.0")

    await registry.ensure_namespace(mode="migrate")

    assert gateway.inserts == []
    expected_query_count = 2
    assert len(gateway.queries) == expected_query_count


@pytest.mark.asyncio
async def test_registry_validate_rejects_missing_namespace_without_write() -> None:
    """Fail missing namespace validation without attempting registration."""
    gateway = ScriptedGateway(query_events=[((RECORDED_AT,),), ()])

    with pytest.raises(ClickHouseNamespaceError, match="contract_missing"):
        await MetadataRegistry(gateway, namespace_contract(), "0.1.0").ensure_namespace(mode="validate")

    assert gateway.inserts == []


@pytest.mark.asyncio
async def test_registry_registers_namespace_then_rereads_exact_row() -> None:
    """Register once with server time and reread the complete frozen row."""
    gateway = ScriptedGateway(
        query_events=[
            ((RECORDED_AT,),),
            (),
            ((RECORDED_AT,),),
            inserted_native_row,
        ],
        insert_events=[None],
    )
    registry = MetadataRegistry(gateway, namespace_contract(), "0.1.0")

    await registry.ensure_namespace(mode="migrate")

    assert len(gateway.inserts) == 1
    expected_query_count = 4
    assert len(gateway.queries) == expected_query_count


@pytest.mark.asyncio
async def test_namespace_registration_rejects_ttl_beyond_current_datetime64_range() -> None:
    """Fail before immutable metadata when current server time cannot fit TTL."""
    almost_maximum = datetime(2299, 12, 31, 23, 59, 59, 999999, tzinfo=UTC) - timedelta(
        microseconds=1,
    )
    gateway = ScriptedGateway(query_events=[((almost_maximum,),)])

    with pytest.raises(ClickHouseNamespaceError, match="retention_unrepresentable") as raised:
        await MetadataRegistry(gateway, namespace_contract(), "0.1.0").ensure_namespace(mode="migrate")

    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None
    assert len(gateway.queries) == 1
    assert gateway.inserts == []


def _record() -> MetadataRecord:
    return MetadataRecordFactory.from_migration(synthetic_plan().migrations[0])
