"""Domain assertions for migration-v1 physical and metadata invariants."""

# ruff: noqa: S101

from clickhouse_connect.driver.asyncclient import AsyncClient
import pytest

from taskiq_clickhouse._clickhouse.adapter import ClickHouseGateway
from taskiq_clickhouse._clickhouse.errors import DefiniteClickHouseError
from taskiq_clickhouse._schema.inspection import SchemaInspector, compare_schema
from taskiq_clickhouse._schema.layout import DDL_SETTINGS
from taskiq_clickhouse._schema.registry import RegistryGateway
from taskiq_clickhouse._storage.layout import StorageLayout
from tests.integration.storage_migration_contract.queries import (
    PROBE_MATERIALIZED_VIEW_INSERT,
    TRUNCATE_TABLE,
)


def assert_metadata_record_bounds(
    evidence: tuple[int, int],
    *,
    max_identities: int,
    max_copies_per_identity: int = 2,
) -> None:
    """Bound unique attempts and physical copies made by two write attempts."""
    row_count, identity_count = evidence
    assert 1 <= identity_count <= max_identities
    assert identity_count <= row_count <= identity_count * max_copies_per_identity


async def assert_physical_contract(client: AsyncClient, layout: StorageLayout) -> None:
    """Require the complete storage schema to match migration-v1 exactly."""
    inspector = SchemaInspector(ClickHouseGateway(client))
    snapshot = await inspector.inspect(layout.target_contract)
    difference = compare_schema(snapshot, layout.target_contract)

    assert difference.matches
    assert difference.mismatches == ()
    assert snapshot.absent_tables == ()
    for contract in layout.target_contract.tables:
        observed = snapshot.table(contract.table)
        assert observed is not None
        assert observed.engine == "MergeTree"
        assert observed.partition_key == "toYYYYMM(purge_at)"
        assert observed.primary_key == "namespace, task_id"
        assert observed.sampling_key == ""
        assert observed.ttl_expression == "purge_at"
        assert all(column.ttl_expression == "" for column in observed.described_columns)


async def assert_result_table_drift_installed(
    gateway: RegistryGateway,
    layout: StorageLayout,
    *,
    expected_path: str,
    expected_actual: str | bool | tuple[str, ...],
) -> None:
    """Require the scenario's exact result-table mismatch to be observable."""
    snapshot = await SchemaInspector(gateway).inspect(layout.target_contract)
    difference = compare_schema(snapshot, layout.target_contract)

    assert snapshot.table(layout.result_table) is not None
    assert difference.matches is False
    observed = tuple(
        (mismatch.path, mismatch.actual) for mismatch in difference.mismatches if mismatch.table == layout.result_table
    )
    assert (expected_path, expected_actual) in observed


async def assert_dependent_view_rejects_insert(
    gateway: RegistryGateway,
    layout: StorageLayout,
) -> None:
    """Prove the dependent view rejects a source insert, then clear its source."""
    with pytest.raises(DefiniteClickHouseError):
        await gateway.command(
            PROBE_MATERIALIZED_VIEW_INSERT.format(
                source=layout.result_table.quoted,
            ),
            settings=DDL_SETTINGS,
        )
    await gateway.command(
        TRUNCATE_TABLE.format(table=layout.result_table.quoted),
        settings=DDL_SETTINGS,
    )
