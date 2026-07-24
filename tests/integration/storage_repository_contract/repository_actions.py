"""Lifecycle and repository construction for real storage scenarios."""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Final
from uuid import uuid4

from clickhouse_connect.driver.asyncclient import AsyncClient

from taskiq_clickhouse._clickhouse.adapter import ClickHouseGateway
from taskiq_clickhouse._clickhouse.contracts import CommandExecutor, ReadWriteGateway
from taskiq_clickhouse._schema.inspection import SchemaInspector
from taskiq_clickhouse._schema.layout import DDL_SETTINGS
from taskiq_clickhouse._schema.records import NamespaceContract
from taskiq_clickhouse._schema.registry import MetadataRegistry
from taskiq_clickhouse._schema.runner import SchemaRunner
from taskiq_clickhouse._storage.layout import (
    StorageLayout,
    build_storage_plan,
    storage_layout_from_names,
)
from taskiq_clickhouse._storage.repository import StorageRepository
from taskiq_clickhouse._storage_policy import (
    NamespaceKey,
    RetentionPolicy,
    StoragePolicy,
)
from taskiq_clickhouse._types import SchemaActor
from tests.integration.fixtures import ClickHouseClientFactory
from tests.integration.storage_repository_contract.gateways import (
    InsertBarrierGateway,
    QueryProjectionGateway,
)


_PACKAGE_VERSION: Final = "integration-test"
_SERIALIZER_ID: Final = "integration-opaque-v1"
_RESULT_TTL_US: Final = 3_600_000_000
_PURGE_TTL_US: Final = 7_200_000_000
_DROP_TABLE: Final = "DROP TABLE IF EXISTS {table} SYNC"


@dataclass(frozen=True, slots=True)
class RepositoryHarness:
    """Disposable migrated layout and its production repository."""

    layout: StorageLayout
    namespace: str
    gateway: ClickHouseGateway
    projection_gateway: QueryProjectionGateway
    repository: StorageRepository


@asynccontextmanager
async def provisioned_repository(
    client: AsyncClient,
    database: str,
    prefix: str,
) -> AsyncIterator[RepositoryHarness]:
    """Yield one migrated repository and always remove its private tables."""
    suffix = f"{prefix}_{uuid4().hex[:10]}"
    namespace = f"integration-{suffix}"
    layout = storage_layout_from_names(
        database,
        f"result_{suffix}",
        f"progress_{suffix}",
    )
    gateway = ClickHouseGateway(client)
    contract = NamespaceContract(
        namespace=namespace,
        result_table=layout.result_table,
        progress_table=layout.progress_table,
        serializer_id=_SERIALIZER_ID,
        result_ttl_us=_RESULT_TTL_US,
        purge_ttl_us=_PURGE_TTL_US,
    )
    await _drop_layout(gateway, layout)
    try:
        await SchemaRunner(
            gateway=gateway,
            inspector=SchemaInspector(gateway),
            registry=MetadataRegistry(
                gateway=gateway,
                namespace_contract=contract,
                package_version=_PACKAGE_VERSION,
            ),
            plan=build_storage_plan(layout),
        ).run(mode="migrate", actor=SchemaActor.WORKER)
        projection_gateway = QueryProjectionGateway(gateway)
        yield RepositoryHarness(
            layout=layout,
            namespace=namespace,
            gateway=gateway,
            projection_gateway=projection_gateway,
            repository=repository(projection_gateway, layout, namespace),
        )
    finally:
        await _drop_layout(gateway, layout)


def repository(
    gateway: ReadWriteGateway,
    layout: StorageLayout,
    namespace: str,
) -> StorageRepository:
    """Bind one storage repository to the integration retention contract."""
    return StorageRepository(
        gateway=gateway,
        layout=layout,
        policy=StoragePolicy(
            namespace=NamespaceKey(namespace),
            retention=RetentionPolicy(_RESULT_TTL_US, _PURGE_TTL_US),
        ),
    )


async def concurrent_repositories(
    client_factory: ClickHouseClientFactory,
    harness: RepositoryHarness,
    writer_count: int,
) -> tuple[StorageRepository, ...]:
    """Open one repository per writer behind a deterministic insert barrier."""
    clients = tuple([await client_factory() for _index in range(writer_count)])
    barrier = asyncio.Barrier(writer_count)
    return tuple(
        repository(
            InsertBarrierGateway(
                ClickHouseGateway(client),
                barrier,
                harness.layout.result_table.table.value,
            ),
            harness.layout,
            harness.namespace,
        )
        for client in clients
    )


async def _drop_layout(gateway: CommandExecutor, layout: StorageLayout) -> None:
    for table in (layout.progress_table, layout.result_table):
        await gateway.command(
            _DROP_TABLE.format(table=table.quoted),
            settings=DDL_SETTINGS,
        )
