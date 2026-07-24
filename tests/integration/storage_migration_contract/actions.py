"""Lifecycle and setup actions for migration-v1 integration scenarios."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import timedelta
from typing import Final
from uuid import uuid4

from clickhouse_connect.driver.asyncclient import AsyncClient

from taskiq_clickhouse._identifiers import Identifier, QualifiedTable
from taskiq_clickhouse._schema.inspection import SchemaInspector
from taskiq_clickhouse._schema.layout import DDL_SETTINGS
from taskiq_clickhouse._schema.records import NamespaceContract
from taskiq_clickhouse._schema.registry import MetadataRegistry, RegistryGateway
from taskiq_clickhouse._schema.runner import SchemaRunner
from taskiq_clickhouse._storage.layout import (
    StorageLayout,
    build_storage_plan,
    storage_layout_from_names,
)
from taskiq_clickhouse.backend import ClickHouseResultBackend
from tests.integration.settings import ClickHouseTestSettings
from tests.integration.storage_migration_contract.cases import PhysicalDriftScenario
from tests.integration.storage_migration_contract.queries import (
    CREATE_DEPENDENT_MATERIALIZED_VIEW,
    CREATE_MATERIALIZED_VIEW_TARGET,
    DROP_TABLE,
)


_PACKAGE_VERSION: Final = "integration-test"
_RESULT_TTL: Final = timedelta(seconds=1)
_PURGE_TTL: Final = timedelta(seconds=2)
_RESULT_TTL_US: Final = 1_000_000
_PURGE_TTL_US: Final = 2_000_000


@dataclass(frozen=True, slots=True)
class MigrationNamespace:
    """Isolated namespace, layout and metadata contract owned by one scenario."""

    layout: StorageLayout
    contract: NamespaceContract


@asynccontextmanager
async def isolated_migration_namespace(
    client: AsyncClient,
    database: str,
    prefix: str,
) -> AsyncIterator[MigrationNamespace]:
    """Own one unique storage layout and remove all related tables on exit."""
    suffix = f"{prefix}_{uuid4().hex[:10]}"
    layout = storage_layout_from_names(
        database,
        f"result_{suffix}",
        f"progress_{suffix}",
    )
    namespace = MigrationNamespace(
        layout=layout,
        contract=NamespaceContract(
            namespace=f"namespace-{suffix}",
            result_table=layout.result_table,
            progress_table=layout.progress_table,
            serializer_id="taskiq-json-v1",
            result_ttl_us=_RESULT_TTL_US,
            purge_ttl_us=_PURGE_TTL_US,
        ),
    )
    await _drop_layout(client, layout)
    try:
        yield namespace
    finally:
        await _drop_layout(client, layout)


def build_backend(
    settings: ClickHouseTestSettings,
    namespace: MigrationNamespace,
) -> ClickHouseResultBackend[object]:
    """Build the public backend from one isolated migration namespace."""
    return ClickHouseResultBackend(
        host=settings.host,
        database=namespace.layout.database.value,
        secure=False,
        result_ttl=_RESULT_TTL,
        purge_ttl=_PURGE_TTL,
        port=settings.port,
        username=settings.username,
        password=settings.password,
        namespace=namespace.contract.namespace,
        result_table=namespace.layout.result_table.table.value,
        progress_table=namespace.layout.progress_table.table.value,
    )


def build_schema_runner(
    gateway: RegistryGateway,
    namespace: MigrationNamespace,
) -> SchemaRunner:
    """Assemble the production runner for an isolated namespace."""
    return SchemaRunner(
        gateway=gateway,
        inspector=SchemaInspector(gateway),
        registry=build_metadata_registry(gateway, namespace),
        plan=build_storage_plan(namespace.layout),
    )


def build_metadata_registry(
    gateway: RegistryGateway,
    namespace: MigrationNamespace,
) -> MetadataRegistry:
    """Build the metadata registry for an isolated namespace."""
    return MetadataRegistry(
        gateway=gateway,
        namespace_contract=namespace.contract,
        package_version=_PACKAGE_VERSION,
    )


async def install_result_table_drift(
    gateway: RegistryGateway,
    layout: StorageLayout,
    scenario: PhysicalDriftScenario,
) -> None:
    """Create one result table and apply the scenario's declarative drift."""
    create_query = layout.create_result_query
    for before, after in scenario.query_replacements:
        create_query = create_query.replace(before, after, 1)
    await gateway.command(
        create_query,
        query_parameters=layout.create_result_parameters,
        settings=DDL_SETTINGS,
    )
    if scenario.alter_query_template is not None:
        await gateway.command(
            scenario.alter_query_template.format(table=layout.result_table.quoted),
            settings=DDL_SETTINGS,
        )
    if scenario.creates_dependent_view:
        await _create_incompatible_materialized_view(gateway, layout)


async def _create_incompatible_materialized_view(
    gateway: RegistryGateway,
    layout: StorageLayout,
) -> None:
    """Install a dependent view whose target rejects every row."""
    view, target = _materialized_view_tables(layout)
    await gateway.command(
        CREATE_MATERIALIZED_VIEW_TARGET.format(target=target.quoted),
        settings=DDL_SETTINGS,
    )
    await gateway.command(
        CREATE_DEPENDENT_MATERIALIZED_VIEW.format(
            view=view.quoted,
            target=target.quoted,
            source=layout.result_table.quoted,
        ),
        settings=DDL_SETTINGS,
    )


async def _drop_layout(client: AsyncClient, layout: StorageLayout) -> None:
    materialized_view, materialized_view_target = _materialized_view_tables(layout)
    for table in (
        materialized_view,
        materialized_view_target,
        layout.progress_table,
        layout.result_table,
    ):
        await client.command(
            DROP_TABLE.format(table=table.quoted),
            settings=dict(DDL_SETTINGS),
        )


def _materialized_view_tables(
    layout: StorageLayout,
) -> tuple[QualifiedTable, QualifiedTable]:
    """Derive test-owned dependent-view names from one unique result table."""
    database = layout.database
    result_name = layout.result_table.table.value
    return (
        QualifiedTable(database, Identifier(f"{result_name}_mv")),
        QualifiedTable(database, Identifier(f"{result_name}_mv_target")),
    )
