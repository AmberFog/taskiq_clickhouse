"""Pure migration-history and namespace integrity checks."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING

from taskiq_clickhouse._schema.layout import (
    MIGRATION_RECORD_KEY,
    MIGRATION_RECORD_KIND,
    NAMESPACE_RECORD_KIND,
    NAMESPACE_RECORD_NAME,
    NAMESPACE_RECORD_VERSION,
)
from taskiq_clickhouse.exceptions import (
    ClickHouseMigrationError,
    ClickHouseNamespaceError,
)


_HISTORY_OPERATION = "migration_history"


if TYPE_CHECKING:
    from collections.abc import Sequence

    from taskiq_clickhouse._schema.migrations import MigrationDefinition, SchemaPlan
    from taskiq_clickhouse._schema.records import MetadataRecord, NamespaceContract


@dataclass(frozen=True, slots=True)
class MigrationHistory:
    """Validated contiguous history for one exact table-set scope."""

    applied_version: int
    records: tuple[MetadataRecord, ...]


def validate_history(
    records: Sequence[MetadataRecord],
    plan: SchemaPlan,
    *,
    expected_scope: str,
) -> MigrationHistory:
    """Validate a complete target-scope history against immutable code."""
    grouped = _group_records(records, expected_scope=expected_scope)
    versions = tuple(sorted(grouped))
    latest_version = _validate_versions(versions, target_version=plan.target_version)
    for version in versions:
        _validate_migration_group(grouped[version], plan.migrations[version - 1])
    return MigrationHistory(applied_version=latest_version, records=tuple(records))


def validate_namespace_records(
    records: Sequence[MetadataRecord],
    contract: NamespaceContract,
) -> None:
    """Accept exact duplicates and reject every conflicting contract row."""
    if not records:
        operation = "namespace_validate"
        reason = "contract_missing"
        raise ClickHouseNamespaceError(operation, reason) from None
    expected = (
        NAMESPACE_RECORD_KIND,
        contract.scope,
        contract.namespace,
        NAMESPACE_RECORD_VERSION,
        NAMESPACE_RECORD_NAME,
        contract.payload_bytes,
        contract.checksum,
    )
    for record in records:
        observed = (
            record.record_kind,
            record.scope,
            record.record_key,
            record.version,
            record.name,
            record.payload,
            record.checksum,
        )
        if observed != expected:
            operation = "namespace_validate"
            reason = "contract_conflict"
            raise ClickHouseNamespaceError(operation, reason) from None


def _validate_migration_identity(record: MetadataRecord, *, expected_scope: str) -> None:
    expected = (MIGRATION_RECORD_KIND, expected_scope, MIGRATION_RECORD_KEY)
    observed = (record.record_kind, record.scope, record.record_key)
    if observed != expected:
        reason = "scope_mismatch"
        raise ClickHouseMigrationError(_HISTORY_OPERATION, reason) from None


def _validate_migration_group(
    records: Sequence[MetadataRecord],
    migration: MigrationDefinition,
) -> None:
    definitions = {(record.name, record.payload, record.checksum) for record in records}
    if len(definitions) != 1:
        reason = "definition_conflict"
        raise ClickHouseMigrationError(_HISTORY_OPERATION, reason) from None
    observed = next(iter(definitions))
    expected = (migration.name, migration.payload_bytes, migration.checksum)
    if observed != expected:
        reason = "definition_changed"
        raise ClickHouseMigrationError(_HISTORY_OPERATION, reason) from None


def _group_records(
    records: Sequence[MetadataRecord],
    *,
    expected_scope: str,
) -> defaultdict[int, list[MetadataRecord]]:
    grouped: defaultdict[int, list[MetadataRecord]] = defaultdict(list)
    for record in records:
        _validate_migration_identity(record, expected_scope=expected_scope)
        grouped[record.version].append(record)
    return grouped


def _validate_versions(versions: tuple[int, ...], *, target_version: int) -> int:
    expected_versions = tuple(range(1, len(versions) + 1))
    if versions != expected_versions:
        reason = "version_gap"
        raise ClickHouseMigrationError(_HISTORY_OPERATION, reason) from None
    latest_version = versions[-1] if versions else 0
    if latest_version > target_version:
        reason = "newer_version"
        raise ClickHouseMigrationError(_HISTORY_OPERATION, reason) from None
    return latest_version
