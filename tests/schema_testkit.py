"""Reusable strict fakes and immutable contracts for schema unit tests."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, TypeAlias, cast

from taskiq_clickhouse._identifiers import Identifier, QualifiedTable
from taskiq_clickhouse._schema.contracts import (
    ColumnContract,
    SchemaContract,
    TableContract,
)
from taskiq_clickhouse._schema.migrations import (
    MigrationDefinition,
    MigrationStep,
    SchemaPlan,
)
from taskiq_clickhouse._schema.records import MetadataRecord, NamespaceContract
from taskiq_clickhouse._types import MigrationExecution
from tests.factories.schema import NamespaceContractFactory


if TYPE_CHECKING:
    from uuid import UUID

    from taskiq_clickhouse._clickhouse.request import InsertRequest


Rows: TypeAlias = tuple[tuple[object, ...], ...]
QueryResponder: TypeAlias = Callable[["ScriptedGateway"], Rows]
QueryEvent: TypeAlias = Rows | BaseException | QueryResponder
OperationEvent: TypeAlias = None | BaseException | Callable[["ScriptedGateway"], None]

RECORDED_AT = datetime(2026, 7, 15, 12, 30, 45, 123456, tzinfo=UTC)


@dataclass(slots=True)
class ScriptedGateway:
    """Execute deterministic query/command/insert events and record issuance."""

    query_events: list[QueryEvent] = field(default_factory=list)
    command_events: list[OperationEvent] = field(default_factory=list)
    insert_events: list[OperationEvent] = field(default_factory=list)
    queries: list[tuple[str, Mapping[str, object] | None, Mapping[str, str] | None]] = field(default_factory=list)
    query_settings: list[Mapping[str, object] | None] = field(default_factory=list)
    commands: list[tuple[str, Mapping[str, object] | None]] = field(default_factory=list)
    inserts: list[InsertRequest] = field(default_factory=list)

    async def query_rows(
        self,
        query: str,
        *,
        query_parameters: Mapping[str, object] | None = None,
        settings: Mapping[str, object] | None = None,
        column_formats: Mapping[str, str] | None = None,
    ) -> Rows:
        """Consume one scripted query event."""
        self.queries.append((query, query_parameters, column_formats))
        self.query_settings.append(settings)
        event = self.query_events.pop(0)
        if isinstance(event, BaseException):
            raise event
        if callable(event):
            return event(self)
        return event

    async def command(
        self,
        query: str,
        *,
        query_parameters: Mapping[str, object] | None = None,
        settings: Mapping[str, object] | None = None,
    ) -> None:
        """Consume one scripted command event."""
        del settings
        self.commands.append((query, query_parameters))
        _run_operation_event(self.command_events.pop(0), self)

    async def insert_rows(self, request: InsertRequest) -> None:
        """Consume one scripted insert event."""
        self.inserts.append(request)
        _run_operation_event(self.insert_events.pop(0), self)


def namespace_contract() -> NamespaceContract:
    """Build the stable namespace fixture used by registry tests."""
    return NamespaceContractFactory.build(
        namespace="test-namespace",
    )


def synthetic_plan(
    *,
    execution: MigrationExecution = MigrationExecution.AUTO,
) -> SchemaPlan:
    """Build one migration with complete absent/present contracts."""
    table = QualifiedTable(Identifier("test_db"), Identifier("synthetic"))
    after = SchemaContract(
        tables=(
            TableContract(
                table=table,
                columns=(ColumnContract(Identifier("value"), "UInt64"),),
                engine="MergeTree",
                partition_key="",
                sorting_key="value",
                primary_key="value",
            ),
        ),
    )
    migration = MigrationDefinition(
        version=1,
        name="create_synthetic",
        execution=execution,
        reentrant=execution is MigrationExecution.AUTO,
        concurrent_safe=execution is MigrationExecution.AUTO,
        steps=(
            MigrationStep(
                ddl="CREATE TABLE IF NOT EXISTS `test_db`.`synthetic` (`value` UInt64) ENGINE=MergeTree ORDER BY value",
                before=SchemaContract(absent_tables=(table,)),
                after=after,
            ),
        ),
    )
    return SchemaPlan((migration,))


def native_row(record: MetadataRecord) -> tuple[object, ...]:
    """Convert a package record into the driver's bytes String projection."""
    row = record.as_row()
    string_indexes = frozenset({0, 1, 2, 4, 6, 7})
    return tuple(
        cell.encode("utf-8") if index in string_indexes and isinstance(cell, str) else cell
        for index, cell in enumerate(row)
    )


def inserted_native_row(fake: ScriptedGateway) -> Rows:
    """Return the last inserted request as one complete driver projection."""
    request = fake.inserts[-1]
    row = request.rows[0]
    record = MetadataRecord(
        record_kind=str(row[0]),
        scope=str(row[1]),
        record_key=str(row[2]),
        version=cast("int", row[3]),
        name=str(row[4]),
        payload=cast("bytes", row[5]),
        checksum=str(row[6]),
        package_version=str(row[7]),
        recorded_at=cast("datetime", row[8]),
        attempt_id=cast("UUID", row[9]),
    )
    return (native_row(record),)


def _run_operation_event(event: OperationEvent, fake: ScriptedGateway) -> None:
    if isinstance(event, BaseException):
        raise event
    if event is not None:
        event(fake)
