"""Permanent metadata registry orchestration."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol
from uuid import uuid4

from taskiq_clickhouse._clickhouse import (
    contracts as clickhouse_contracts,
    errors as clickhouse_errors,
    queries as clickhouse_queries,
)
from taskiq_clickhouse._schema import codec, integrity, layout, ports, records, transport
from taskiq_clickhouse._sql import bind_table
from taskiq_clickhouse._write_acknowledgement import AttemptOutcome
from taskiq_clickhouse.exceptions import (
    ClickHouseBackendIOError,
    ClickHouseNamespaceError,
)


_NAMESPACE_VALIDATE_OPERATION = "namespace_validate"
_CONTRACT_MISSING_REASON = "contract_missing"
_RETENTION_UNREPRESENTABLE_REASON = "retention_unrepresentable"


if TYPE_CHECKING:
    from datetime import datetime

    from taskiq_clickhouse._schema.migrations import MigrationDefinition, SchemaPlan
    from taskiq_clickhouse._types import SchemaMode


class RegistryGateway(
    clickhouse_contracts.ReadWriteGateway,
    clickhouse_contracts.CommandExecutor,
    Protocol,
):
    """ClickHouse capabilities required by permanent metadata policy."""


@dataclass(frozen=True, slots=True)
class _RegistryReader:
    """Read strict metadata rows and allocate frozen server-time identities."""

    gateway: RegistryGateway
    layout: layout.MetadataLayout
    namespace_contract: records.NamespaceContract
    package_version: str

    async def read_records(
        self,
        *,
        record_kind: str,
        record_key: str,
        operation: str,
    ) -> tuple[records.MetadataRecord, ...]:
        """Read every row for one exact kind/scope/key prefix."""
        rows = await clickhouse_queries.query_rows(
            self.gateway,
            clickhouse_queries.QueryRequest(
                query=self.layout.read_query,
                operation=operation,
                query_parameters=bind_table(
                    self.layout.table,
                    {
                        "record_kind": record_kind,
                        "record_key": record_key,
                        "scope": self.namespace_contract.scope,
                    },
                ),
                column_formats=layout.STRING_COLUMN_FORMATS,
            ),
        )
        return codec.parse_records(rows, operation=operation)

    async def new_record(self, definition: _RecordDefinition) -> records.MetadataRecord:
        """Allocate server-observed time and one random UUIDv4 exactly once."""
        recorded_at = await self.server_time()
        return records.MetadataRecord(
            record_kind=definition.record_kind,
            scope=self.namespace_contract.scope,
            record_key=definition.record_key,
            version=definition.version,
            name=definition.name,
            payload=definition.payload,
            checksum=definition.checksum,
            package_version=self.package_version,
            recorded_at=recorded_at,
            attempt_id=uuid4(),
        )

    async def server_time(self) -> datetime:
        """Read one authoritative UTC timestamp through the safe query boundary."""
        rows = await clickhouse_queries.query_rows(
            self.gateway,
            clickhouse_queries.QueryRequest(
                query=layout.SERVER_NOW_QUERY,
                operation="metadata_clock_read",
            ),
        )
        return codec.parse_server_time(rows)


@dataclass(frozen=True, slots=True)
class _RecordDefinition:
    """Immutable metadata payload before server time and attempt allocation."""

    record_kind: str
    record_key: str
    version: int
    name: str
    payload: bytes
    checksum: str


@dataclass(frozen=True, slots=True)
class MetadataRegistry:  # noqa: WPS214 - one cohesive implementation of the complete SchemaRegistry port.
    """Manage one backend's permanent append-only package metadata."""

    gateway: RegistryGateway
    namespace_contract: records.NamespaceContract
    package_version: str

    def __post_init__(self) -> None:
        """Reject invalid local construction before issuing I/O."""
        if not isinstance(self.package_version, str) or not self.package_version:
            msg = "package_version must be a non-empty string"
            raise ValueError(msg)

    @property
    def layout(self) -> layout.MetadataLayout:
        """Return the fixed metadata layout in the configured database."""
        return layout.MetadataLayout(self.namespace_contract.result_table.database)

    async def bootstrap(self, verifier: ports.SchemaVerifier, *, mode: SchemaMode) -> None:
        """Create when allowed, then validate the exact physical registry."""
        if mode == "validate":
            await verifier.validate(self.layout.contract)
            return
        outcome = await _create_metadata_table(self.gateway, self.layout)
        if outcome is AttemptOutcome.AMBIGUOUS:
            matches = await verifier.matches(self.layout.contract)
        else:
            matches = True
        if not matches:
            operation = "metadata_bootstrap"
            reason = "ddl_unconfirmed"
            raise ClickHouseBackendIOError(operation, reason) from None
        await verifier.validate(self.layout.contract)

    async def read_history(self, plan: SchemaPlan) -> integrity.MigrationHistory:
        """Read and validate all migration evidence for the exact scope."""
        records = await self._reader().read_records(
            record_kind=layout.MIGRATION_RECORD_KIND,
            record_key=layout.MIGRATION_RECORD_KEY,
            operation="migration_history_read",
        )
        return integrity.validate_history(
            records,
            plan,
            expected_scope=self.namespace_contract.scope,
        )

    async def validate_retention(self) -> None:
        """Fail before schema writes when the current server clock cannot fit TTL."""
        retention_observed_at = await self._reader().server_time()
        _validate_retention_window(self.namespace_contract, retention_observed_at)

    async def record_migration(self, migration: MigrationDefinition) -> None:
        """Append acknowledged success evidence for one verified migration."""
        record = await self._reader().new_record(
            _RecordDefinition(
                record_kind=layout.MIGRATION_RECORD_KIND,
                record_key=layout.MIGRATION_RECORD_KEY,
                version=migration.version,
                name=migration.name,
                payload=migration.payload_bytes,
                checksum=migration.checksum,
            ),
        )
        await transport.ExactMetadataWriter(self.gateway, self.layout).write(
            record,
            operation="migration_record_write",
        )

    async def ensure_namespace(self, *, mode: SchemaMode) -> None:
        """Read first, optionally register once, then fail on every conflict."""
        reader = self._reader()
        await self.validate_retention()
        records = await reader.read_records(
            record_kind=layout.NAMESPACE_RECORD_KIND,
            record_key=self.namespace_contract.namespace,
            operation="namespace_read",
        )
        if records:
            integrity.validate_namespace_records(records, self.namespace_contract)
            return
        if mode == "validate":
            raise ClickHouseNamespaceError(
                _NAMESPACE_VALIDATE_OPERATION,
                _CONTRACT_MISSING_REASON,
            ) from None
        record = await reader.new_record(
            _RecordDefinition(
                record_kind=layout.NAMESPACE_RECORD_KIND,
                record_key=self.namespace_contract.namespace,
                version=layout.NAMESPACE_RECORD_VERSION,
                name=layout.NAMESPACE_RECORD_NAME,
                payload=self.namespace_contract.payload_bytes,
                checksum=self.namespace_contract.checksum,
            ),
        )
        await transport.ExactMetadataWriter(self.gateway, self.layout).write(
            record,
            operation="namespace_record_write",
        )
        final_records = await reader.read_records(
            record_kind=layout.NAMESPACE_RECORD_KIND,
            record_key=self.namespace_contract.namespace,
            operation="namespace_read",
        )
        integrity.validate_namespace_records(final_records, self.namespace_contract)

    def _reader(self) -> _RegistryReader:
        return _RegistryReader(
            gateway=self.gateway,
            layout=self.layout,
            namespace_contract=self.namespace_contract,
            package_version=self.package_version,
        )


async def _create_metadata_table(
    gateway: clickhouse_contracts.CommandExecutor,
    metadata_layout: layout.MetadataLayout,
) -> AttemptOutcome:
    error_reason: str | None = None
    try:
        await gateway.command(
            metadata_layout.create_query,
            query_parameters=metadata_layout.table_parameters,
            settings=layout.DDL_SETTINGS,
        )
    except clickhouse_errors.AmbiguousClickHouseError:
        return AttemptOutcome.AMBIGUOUS
    except clickhouse_errors.DefiniteClickHouseError:
        error_reason = "database_error"
    if error_reason is not None:
        operation = "metadata_bootstrap"
        raise ClickHouseBackendIOError(operation, error_reason) from None
    return AttemptOutcome.ACKNOWLEDGED


def _validate_retention_window(
    contract: records.NamespaceContract,
    observed_at: object,
) -> None:
    failure: ClickHouseNamespaceError | None = None
    try:
        contract.require_retention_feasible_at(observed_at)
    except (TypeError, ValueError):
        failure = ClickHouseNamespaceError(
            _NAMESPACE_VALIDATE_OPERATION,
            _RETENTION_UNREPRESENTABLE_REASON,
        )
    if failure is not None:
        raise failure from None
