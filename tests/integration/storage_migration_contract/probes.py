"""Read-only ClickHouse observations for migration-v1 scenarios."""

from clickhouse_connect.driver.asyncclient import AsyncClient

from taskiq_clickhouse._schema.layout import (
    MIGRATION_RECORD_KEY,
    MIGRATION_RECORD_KIND,
    NAMESPACE_RECORD_KIND,
    MetadataLayout,
)
from taskiq_clickhouse._schema.records import NamespaceContract
from taskiq_clickhouse._storage.layout import StorageLayout
from tests.integration.storage_migration_contract.queries import (
    COUNT_METADATA,
    COUNT_ROWS,
)


async def metadata_evidence(
    client: AsyncClient,
    contract: NamespaceContract,
) -> tuple[tuple[int, int], tuple[int, int]]:
    """Return row/identity counts for migration and namespace records."""
    migration = await _metadata_count(
        client,
        contract,
        record_kind=MIGRATION_RECORD_KIND,
        record_key=MIGRATION_RECORD_KEY,
    )
    namespace = await _metadata_count(
        client,
        contract,
        record_kind=NAMESPACE_RECORD_KIND,
        record_key=contract.namespace,
    )
    return migration, namespace


async def storage_row_counts(
    client: AsyncClient,
    layout: StorageLayout,
) -> tuple[int, int]:
    """Return exact result/progress row counts from the drifted layout."""
    result_response = await client.query(COUNT_ROWS.format(table=layout.result_table.quoted))
    progress_response = await client.query(COUNT_ROWS.format(table=layout.progress_table.quoted))
    return (
        _count(result_response.result_rows[0][0]),
        _count(progress_response.result_rows[0][0]),
    )


async def _metadata_count(
    client: AsyncClient,
    contract: NamespaceContract,
    *,
    record_kind: str,
    record_key: str,
) -> tuple[int, int]:
    metadata_table = MetadataLayout(contract.result_table.database).table
    response = await client.query(
        COUNT_METADATA.format(table=metadata_table.quoted),
        parameters={
            "record_kind": record_kind,
            "scope": contract.scope,
            "record_key": record_key,
        },
    )
    return _count(response.result_rows[0][0]), _count(response.result_rows[0][1])


def _count(candidate: object) -> int:
    if not isinstance(candidate, int) or isinstance(candidate, bool):
        message = "ClickHouse count query returned a non-integer"
        raise TypeError(message)
    return candidate
