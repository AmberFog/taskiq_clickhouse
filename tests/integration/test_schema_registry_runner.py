"""Exercise the complete schema registry and runner against real ClickHouse."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field, replace
from datetime import timedelta
from typing import TYPE_CHECKING, Final
from uuid import uuid4

import pytest

from taskiq_clickhouse._clickhouse.adapter import ClickHouseGateway
from taskiq_clickhouse._clickhouse.errors import AmbiguousClickHouseError
from taskiq_clickhouse._identifiers import METADATA_TABLE_NAME, Identifier, QualifiedTable
from taskiq_clickhouse._schema.contracts import (
    ColumnContract,
    SchemaContract,
    TableContract,
)
from taskiq_clickhouse._schema.inspection import SchemaInspector
from taskiq_clickhouse._schema.layout import (
    DDL_SETTINGS,
    MIGRATION_RECORD_KEY,
    MIGRATION_RECORD_KIND,
    NAMESPACE_RECORD_KIND,
    MetadataLayout,
)
from taskiq_clickhouse._schema.migrations import (
    MigrationDefinition,
    MigrationStep,
    SchemaPlan,
)
from taskiq_clickhouse._schema.records import NamespaceContract
from taskiq_clickhouse._schema.registry import MetadataRegistry, RegistryGateway
from taskiq_clickhouse._schema.runner import SchemaRunner
from taskiq_clickhouse._types import MigrationExecution, SchemaActor
from taskiq_clickhouse.backend import ClickHouseResultBackend
from taskiq_clickhouse.exceptions import (
    ClickHouseNamespaceError,
    ClickHouseSchemaDriftError,
)
from taskiq_clickhouse.schema import ClickHouseSchemaManager


if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping

    from clickhouse_connect.driver.asyncclient import AsyncClient

    from taskiq_clickhouse._clickhouse.request import InsertRequest
    from tests.integration.fixtures import ClickHouseClientFactory
    from tests.integration.settings import ClickHouseTestSettings


pytestmark = [pytest.mark.clickhouse, pytest.mark.asyncio]

_PACKAGE_VERSION: Final = "integration-test"
_DROP_TABLE: Final = "DROP TABLE IF EXISTS {table} SYNC"
_WRONG_METADATA_DDL: Final = """
CREATE TABLE {table}
(
    record_kind String
)
ENGINE = MergeTree
ORDER BY record_kind
"""
_WRONG_SYNTHETIC_DDL: Final = """
CREATE TABLE {table}
(
    value String
)
ENGINE = MergeTree
ORDER BY value
"""
_COUNT_METADATA: Final = """
SELECT count()
FROM {table}
WHERE record_kind = {{record_kind:String}}
  AND scope = {{scope:String}}
  AND record_key = {{record_key:String}}
"""
_COUNT_METADATA_IDENTITIES: Final = """
SELECT uniqExact(attempt_id)
FROM {table}
WHERE record_kind = {{record_kind:String}}
  AND scope = {{scope:String}}
  AND record_key = {{record_key:String}}
"""
_CONFIRMATION_MARKER: Final = "attempt_id = {attempt_id:UUID}"
_MIGRATION_AND_NAMESPACE_WRITES: Final = 2
_CONFLICTING_NAMESPACE_ROWS: Final = 2
_RESULT_TTL: Final = timedelta(seconds=1)
_PURGE_TTL: Final = timedelta(seconds=2)


@dataclass(slots=True)
class _GatewayProbe:
    """Record real I/O and inject one bounded namespace-write event."""

    delegate: RegistryGateway
    pause_namespace_insert: bool = False
    lose_namespace_response: bool = False
    lose_migration_response: bool = False
    lose_ddl_response: bool = False
    after_namespace_insert: Callable[[], Awaitable[None]] | None = None
    query_count: int = 0
    command_count: int = 0
    insert_count: int = 0
    confirmation_count: int = 0
    namespace_insert_reached: asyncio.Event = field(default_factory=asyncio.Event)
    resume_namespace_insert: asyncio.Event = field(default_factory=asyncio.Event)

    async def query_rows(
        self,
        query: str,
        *,
        query_parameters: Mapping[str, object] | None = None,
        settings: Mapping[str, object] | None = None,
        column_formats: Mapping[str, str] | None = None,
    ) -> tuple[tuple[object, ...], ...]:
        """Forward one real read while counting exact confirmations."""
        self.query_count += 1
        if _CONFIRMATION_MARKER in query:
            self.confirmation_count += 1
        return await self.delegate.query_rows(
            query,
            query_parameters=query_parameters,
            settings=settings,
            column_formats=column_formats,
        )

    async def command(
        self,
        query: str,
        *,
        query_parameters: Mapping[str, object] | None = None,
        settings: Mapping[str, object] | None = None,
    ) -> None:
        """Forward and count one real DDL command."""
        self.command_count += 1
        await self.delegate.command(
            query,
            query_parameters=query_parameters,
            settings=settings,
        )
        is_migration_ddl = METADATA_TABLE_NAME not in query and query.lstrip().startswith(("CREATE", "ALTER"))
        if is_migration_ddl and self.lose_ddl_response:
            self.lose_ddl_response = False
            raise AmbiguousClickHouseError

    async def insert_rows(self, request: InsertRequest) -> None:
        """Optionally pause or lose the response after a real namespace insert."""
        self.insert_count += 1
        is_namespace = _is_namespace_insert(request)
        if is_namespace and self.pause_namespace_insert:
            self.pause_namespace_insert = False
            self.namespace_insert_reached.set()
            await self.resume_namespace_insert.wait()
        await self.delegate.insert_rows(request)
        callback = self.after_namespace_insert
        if is_namespace and callback is not None:
            self.after_namespace_insert = None
            await callback()
        if is_namespace and self.lose_namespace_response:
            self.lose_namespace_response = False
            raise AmbiguousClickHouseError
        if _is_migration_insert(request) and self.lose_migration_response:
            self.lose_migration_response = False
            raise AmbiguousClickHouseError


@pytest.mark.asyncio
async def test_validate_absent_registry_is_write_free(
    clickhouse_client: AsyncClient,
    clickhouse_database: str,
) -> None:
    """Fail an absent validate-only registry after reads and before any mutation."""
    metadata_layout = MetadataLayout(Identifier(clickhouse_database))
    await _drop_table(clickhouse_client, metadata_layout.table)
    contract = _namespace_contract(clickhouse_database, _unique_suffix("validate_absent"))
    probe = _GatewayProbe(ClickHouseGateway(clickhouse_client))

    with pytest.raises(ClickHouseSchemaDriftError):
        await _runner(probe, contract, SchemaPlan(())).run(
            mode="validate",
            actor=SchemaActor.WORKER,
        )

    assert probe.query_count == 1
    assert probe.command_count == 0
    assert probe.insert_count == 0


@pytest.mark.asyncio
async def test_existing_wrong_metadata_schema_fails_after_if_not_exists(
    clickhouse_client: AsyncClient,
    clickhouse_database: str,
) -> None:
    """Prove acknowledged IF NOT EXISTS never substitutes for physical validation."""
    metadata_layout = MetadataLayout(Identifier(clickhouse_database))
    await _drop_table(clickhouse_client, metadata_layout.table)
    await clickhouse_client.command(
        _WRONG_METADATA_DDL.format(table=metadata_layout.table.quoted),
        settings=dict(DDL_SETTINGS),
    )
    contract = _namespace_contract(clickhouse_database, _unique_suffix("wrong_metadata"))
    probe = _GatewayProbe(ClickHouseGateway(clickhouse_client))
    try:
        with pytest.raises(ClickHouseSchemaDriftError):
            await _runner(probe, contract, SchemaPlan(())).run(
                mode="migrate",
                actor=SchemaActor.WORKER,
            )

        assert probe.command_count == 1
        assert probe.insert_count == 0
    finally:
        await _drop_table(clickhouse_client, metadata_layout.table)


@pytest.mark.asyncio
async def test_auto_vertical_path_validate_and_read_first_restart_do_not_grow(
    clickhouse_client: AsyncClient,
    clickhouse_database: str,
) -> None:
    """Apply AUTO end to end, then prove validate and identical restart append nothing."""
    suffix = _unique_suffix("vertical")
    contract = _namespace_contract(clickhouse_database, suffix)
    plan = _synthetic_plan(clickhouse_database, suffix)
    target_table = plan.migrations[0].target.tables[0].table
    await _drop_table(clickhouse_client, target_table)
    try:
        initial_probe = _GatewayProbe(ClickHouseGateway(clickhouse_client))
        await _runner(initial_probe, contract, plan).run(
            mode="migrate",
            actor=SchemaActor.WORKER,
        )
        initial_counts = await _evidence_counts(clickhouse_client, contract)

        validate_probe = _GatewayProbe(ClickHouseGateway(clickhouse_client))
        await _runner(validate_probe, contract, plan).run(
            mode="validate",
            actor=SchemaActor.WORKER,
        )
        restart_probe = _GatewayProbe(ClickHouseGateway(clickhouse_client))
        await _runner(restart_probe, contract, plan).run(
            mode="migrate",
            actor=SchemaActor.WORKER,
        )

        assert initial_probe.command_count == _MIGRATION_AND_NAMESPACE_WRITES
        assert initial_probe.insert_count == _MIGRATION_AND_NAMESPACE_WRITES
        assert initial_counts == (1, 1)
        assert validate_probe.command_count == 0
        assert validate_probe.insert_count == 0
        assert restart_probe.command_count == 1
        assert restart_probe.insert_count == 0
        assert await _evidence_counts(clickhouse_client, contract) == initial_counts
    finally:
        await _drop_table(clickhouse_client, target_table)


@pytest.mark.asyncio
async def test_ddl_before_history_is_recovered_without_ddl_reexecution(
    clickhouse_client: AsyncClient,
    clickhouse_database: str,
) -> None:
    """Recover a crash after physical DDL by recording its already-proven target."""
    suffix = _unique_suffix("ddl_recovery")
    contract = _namespace_contract(clickhouse_database, suffix)
    plan = _synthetic_plan(clickhouse_database, suffix)
    migration = plan.migrations[0]
    target_table = migration.target.tables[0].table
    base_gateway = ClickHouseGateway(clickhouse_client)
    registry = _registry(base_gateway, contract)
    await registry.bootstrap(SchemaInspector(base_gateway), mode="migrate")
    await base_gateway.command(migration.steps[0].ddl, settings=DDL_SETTINGS)
    try:
        probe = _GatewayProbe(base_gateway)
        await _runner(probe, contract, plan).run(
            mode="migrate",
            actor=SchemaActor.WORKER,
        )

        assert probe.command_count == 1
        assert probe.insert_count == _MIGRATION_AND_NAMESPACE_WRITES
        assert await _evidence_counts(clickhouse_client, contract) == (1, 1)
    finally:
        await _drop_table(clickhouse_client, target_table)


@pytest.mark.asyncio
async def test_forged_current_history_cannot_mask_physical_drift(
    clickhouse_client: AsyncClient,
    clickhouse_database: str,
) -> None:
    """Reject a current history record when the physical target has a wrong type."""
    suffix = _unique_suffix("forged_history")
    contract = _namespace_contract(clickhouse_database, suffix)
    plan = _synthetic_plan(clickhouse_database, suffix)
    migration = plan.migrations[0]
    target_table = migration.target.tables[0].table
    gateway = ClickHouseGateway(clickhouse_client)
    registry = _registry(gateway, contract)
    await registry.bootstrap(SchemaInspector(gateway), mode="migrate")
    await registry.record_migration(migration)
    await gateway.command(
        _WRONG_SYNTHETIC_DDL.format(table=target_table.quoted),
        settings=DDL_SETTINGS,
    )
    try:
        with pytest.raises(ClickHouseSchemaDriftError):
            await _runner(gateway, contract, plan).run(
                mode="migrate",
                actor=SchemaActor.WORKER,
            )

        assert (
            await _metadata_count(
                clickhouse_client,
                contract,
                record_kind=MIGRATION_RECORD_KIND,
                record_key=MIGRATION_RECORD_KEY,
            )
            == 1
        )
        assert (
            await _metadata_count(
                clickhouse_client,
                contract,
                record_kind=NAMESPACE_RECORD_KIND,
                record_key=contract.namespace,
            )
            == 0
        )
    finally:
        await _drop_table(clickhouse_client, target_table)


@pytest.mark.asyncio
async def test_twenty_independent_runners_converge_and_restarts_do_not_grow(
    clickhouse_clients: tuple[AsyncClient, ...],
    clickhouse_database: str,
) -> None:
    """Converge concurrent bootstrap, AUTO migration and namespace registration."""
    suffix = _unique_suffix("twenty")
    contract = _namespace_contract(clickhouse_database, suffix)
    plan = _two_version_plan(clickhouse_database, suffix)
    target_tables = tuple(table_contract.table for table_contract in plan.migrations[-1].target.tables)
    metadata_table = MetadataLayout(Identifier(clickhouse_database)).table
    await _drop_table(clickhouse_clients[0], metadata_table)
    for target_table in target_tables:
        await _drop_table(clickhouse_clients[0], target_table)
    barrier = asyncio.Barrier(len(clickhouse_clients))

    async def run_participant(client: AsyncClient) -> None:
        await barrier.wait()
        gateway = ClickHouseGateway(client)
        await _runner(gateway, contract, plan).run(
            mode="migrate",
            actor=SchemaActor.WORKER,
        )

    try:
        async with asyncio.timeout(15):
            await asyncio.gather(*(run_participant(client) for client in clickhouse_clients))
        concurrent_counts = await _evidence_counts(clickhouse_clients[0], contract)
        identity_counts = await _evidence_identity_counts(clickhouse_clients[0], contract)

        for _restart in range(3):
            gateway = ClickHouseGateway(clickhouse_clients[0])
            await _runner(gateway, contract, plan).run(
                mode="migrate",
                actor=SchemaActor.WORKER,
            )

        migration_identities, namespace_identities = identity_counts
        assert plan.target_version <= migration_identities <= len(clickhouse_clients) * plan.target_version
        assert 1 <= namespace_identities <= len(clickhouse_clients)
        assert await _evidence_counts(clickhouse_clients[0], contract) == concurrent_counts
        assert await _evidence_identity_counts(clickhouse_clients[0], contract) == identity_counts
    finally:
        for target_table in target_tables:
            await _drop_table(clickhouse_clients[0], target_table)


@pytest.mark.asyncio
async def test_orchestrated_namespace_conflict_poisoning(
    clickhouse_client_factory: ClickHouseClientFactory,
    clickhouse_database: str,
) -> None:
    """Let A pass before B publishes its stale-read conflict, then poison future starts."""
    client_a = await clickhouse_client_factory()
    client_b = await clickhouse_client_factory()
    suffix = _unique_suffix("namespace_conflict")
    contract_a = _namespace_contract(clickhouse_database, suffix)
    contract_b = replace(contract_a, serializer_id="taskiq-json-v2")
    plan = SchemaPlan(())
    gateway_a = ClickHouseGateway(client_a)
    await _registry(gateway_a, contract_a).bootstrap(
        SchemaInspector(gateway_a),
        mode="migrate",
    )
    probe_b = _GatewayProbe(
        ClickHouseGateway(client_b),
        pause_namespace_insert=True,
    )
    runner_b = _runner(probe_b, contract_b, plan)
    task_b = asyncio.create_task(
        runner_b.run(mode="migrate", actor=SchemaActor.WORKER),
    )

    async with asyncio.timeout(10):
        await probe_b.namespace_insert_reached.wait()
    try:
        await _runner(gateway_a, contract_a, plan).run(
            mode="migrate",
            actor=SchemaActor.WORKER,
        )
    finally:
        probe_b.resume_namespace_insert.set()

    with pytest.raises(ClickHouseNamespaceError, match="contract_conflict"):
        await task_b
    with pytest.raises(ClickHouseNamespaceError, match="contract_conflict"):
        await _runner(gateway_a, contract_a, plan).run(
            mode="validate",
            actor=SchemaActor.WORKER,
        )

    assert (
        await _metadata_count(
            client_a,
            contract_a,
            record_kind=NAMESPACE_RECORD_KIND,
            record_key=contract_a.namespace,
        )
        == _CONFLICTING_NAMESPACE_ROWS
    )


@pytest.mark.asyncio
async def test_real_insert_then_lost_response_is_confirmed_exactly(
    clickhouse_client: AsyncClient,
    clickhouse_database: str,
) -> None:
    """Confirm a real committed metadata row after one simulated lost response."""
    contract = _namespace_contract(clickhouse_database, _unique_suffix("lost_response"))
    probe = _GatewayProbe(
        ClickHouseGateway(clickhouse_client),
        lose_namespace_response=True,
    )

    await _runner(probe, contract, SchemaPlan(())).run(
        mode="migrate",
        actor=SchemaActor.WORKER,
    )

    assert probe.insert_count == 1
    assert probe.confirmation_count == 1
    assert (
        await _metadata_count(
            clickhouse_client,
            contract,
            record_kind=NAMESPACE_RECORD_KIND,
            record_key=contract.namespace,
        )
        == 1
    )


@pytest.mark.asyncio
async def test_real_ddl_and_migration_insert_lost_responses_recover(
    clickhouse_client: AsyncClient,
    clickhouse_database: str,
) -> None:
    """Use physical and exact-row postconditions after two committed losses."""
    suffix = _unique_suffix("migration_response_loss")
    contract = _namespace_contract(clickhouse_database, suffix)
    plan = _synthetic_plan(clickhouse_database, suffix)
    target_table = plan.migrations[-1].target.tables[0].table
    probe = _GatewayProbe(
        ClickHouseGateway(clickhouse_client),
        lose_migration_response=True,
        lose_ddl_response=True,
    )
    try:
        await _runner(probe, contract, plan).run(
            mode="migrate",
            actor=SchemaActor.WORKER,
        )

        assert probe.lose_ddl_response is False
        assert probe.lose_migration_response is False
        assert probe.confirmation_count == 1
        assert await _evidence_counts(clickhouse_client, contract) == (1, 1)
    finally:
        await _drop_table(clickhouse_client, target_table)


@pytest.mark.asyncio
async def test_final_barrier_rejects_metadata_drift_after_namespace_insert(
    clickhouse_client: AsyncClient,
    clickhouse_database: str,
) -> None:
    """Reinspect permanent metadata after a successful namespace write."""
    contract = _namespace_contract(clickhouse_database, _unique_suffix("metadata_drift"))
    metadata_table = MetadataLayout(Identifier(clickhouse_database)).table

    async def inject_drift() -> None:
        await clickhouse_client.command(
            f"ALTER TABLE {metadata_table.quoted} ADD COLUMN `unexpected_metadata` UInt8 DEFAULT 0",
            settings=dict(DDL_SETTINGS),
        )

    probe = _GatewayProbe(
        ClickHouseGateway(clickhouse_client),
        after_namespace_insert=inject_drift,
    )
    try:
        with pytest.raises(ClickHouseSchemaDriftError):
            await _runner(probe, contract, SchemaPlan(())).run(
                mode="migrate",
                actor=SchemaActor.WORKER,
            )
    finally:
        await clickhouse_client.command(
            f"ALTER TABLE {metadata_table.quoted} DROP COLUMN IF EXISTS `unexpected_metadata`",
            settings=dict(DDL_SETTINGS),
        )


@pytest.mark.asyncio
async def test_public_manager_preprovisions_for_validate_only_user(
    clickhouse_client: AsyncClient,
    clickhouse_database: str,
    clickhouse_settings: ClickHouseTestSettings,
) -> None:
    """Cross the public barrier with a worker that has no DDL or INSERT grant."""
    suffix = _unique_suffix("least_privilege")
    username = Identifier(f"validate_{uuid4().hex[:12]}")
    admin_backend = _integration_backend(
        clickhouse_settings,
        clickhouse_database,
        suffix,
        username=clickhouse_settings.username,
        password=clickhouse_settings.password,
    )
    await ClickHouseSchemaManager(admin_backend).migrate()
    metadata_table = MetadataLayout(Identifier(clickhouse_database)).table

    await clickhouse_client.command(f"CREATE USER {username.quoted}")
    try:
        for grant in (
            f"GRANT SELECT ON {metadata_table.quoted} TO {username.quoted}",
            f"GRANT SELECT ON `system`.`tables` TO {username.quoted}",
            f"GRANT SELECT ON `system`.`columns` TO {username.quoted}",
            f"GRANT SELECT ON `system`.`data_skipping_indices` TO {username.quoted}",
            f"GRANT SELECT ON `system`.`projections` TO {username.quoted}",
            f"GRANT SHOW COLUMNS ON {Identifier(clickhouse_database).quoted}.* TO {username.quoted}",
        ):
            await clickhouse_client.command(grant)
        worker_backend = _integration_backend(
            clickhouse_settings,
            clickhouse_database,
            suffix,
            username=username.value,
            password="",
        )

        await ClickHouseSchemaManager(worker_backend).validate()
    finally:
        await clickhouse_client.command(f"DROP USER IF EXISTS {username.quoted}")


def _runner(
    gateway: RegistryGateway,
    contract: NamespaceContract,
    plan: SchemaPlan,
) -> SchemaRunner:
    return SchemaRunner(
        gateway=gateway,
        inspector=SchemaInspector(gateway),
        registry=_registry(gateway, contract),
        plan=plan,
    )


def _registry(
    gateway: RegistryGateway,
    contract: NamespaceContract,
) -> MetadataRegistry:
    return MetadataRegistry(
        gateway=gateway,
        namespace_contract=contract,
        package_version=_PACKAGE_VERSION,
    )


def _namespace_contract(
    database_name: str,
    suffix: str,
) -> NamespaceContract:
    database = Identifier(database_name)
    return NamespaceContract(
        namespace=f"namespace-{suffix}",
        result_table=QualifiedTable(database, Identifier(f"result_{suffix}")),
        progress_table=QualifiedTable(database, Identifier(f"progress_{suffix}")),
        serializer_id="taskiq-json-v1",
        result_ttl_us=1_000_000,
        purge_ttl_us=2_000_000,
    )


def _integration_backend(
    settings: ClickHouseTestSettings,
    database: str,
    suffix: str,
    *,
    username: str,
    password: str,
) -> ClickHouseResultBackend[object]:
    return ClickHouseResultBackend(
        host=settings.host,
        database=database,
        secure=False,
        result_ttl=_RESULT_TTL,
        purge_ttl=_PURGE_TTL,
        port=settings.port,
        username=username,
        password=password,
        namespace=f"namespace-{suffix}",
        result_table=f"result_{suffix}",
        progress_table=f"progress_{suffix}",
        schema_mode="validate",
    )


def _synthetic_plan(database_name: str, suffix: str) -> SchemaPlan:
    table = QualifiedTable(
        Identifier(database_name),
        Identifier(f"synthetic_{suffix}"),
    )
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
        execution=MigrationExecution.AUTO,
        reentrant=True,
        concurrent_safe=True,
        steps=(
            MigrationStep(
                ddl=(f"CREATE TABLE IF NOT EXISTS {table.quoted} (`value` UInt64) ENGINE=MergeTree ORDER BY value"),
                before=SchemaContract(absent_tables=(table,)),
                after=after,
            ),
        ),
    )
    return SchemaPlan((migration,))


def _two_version_plan(database_name: str, suffix: str) -> SchemaPlan:
    first = _synthetic_plan(database_name, suffix).migrations[0]
    primary_table = first.target.tables[0]
    extended_table = replace(
        primary_table,
        columns=(
            *primary_table.columns,
            ColumnContract(
                Identifier("extra"),
                "UInt64",
                default_kind="DEFAULT",
                default_expression="0",
            ),
        ),
    )
    after_alter = SchemaContract(tables=(extended_table,))
    audit_table = QualifiedTable(
        Identifier(database_name),
        Identifier(f"synthetic_audit_{suffix}"),
    )
    audit_contract = TableContract(
        table=audit_table,
        columns=(ColumnContract(Identifier("sequence"), "UInt64"),),
        engine="MergeTree",
        partition_key="",
        sorting_key="sequence",
        primary_key="sequence",
    )
    final_target = SchemaContract(tables=(extended_table, audit_contract))
    primary_name = primary_table.table.quoted
    second = MigrationDefinition(
        version=2,
        name="extend_synthetic",
        execution=MigrationExecution.AUTO,
        reentrant=True,
        concurrent_safe=True,
        steps=(
            MigrationStep(
                ddl=f"ALTER TABLE {primary_name} ADD COLUMN IF NOT EXISTS `extra` UInt64 DEFAULT 0",
                before=first.target,
                after=after_alter,
            ),
            MigrationStep(
                ddl=(
                    f"CREATE TABLE IF NOT EXISTS {audit_table.quoted} (`sequence` UInt64) "
                    "ENGINE=MergeTree ORDER BY sequence"
                ),
                before=after_alter,
                after=final_target,
            ),
        ),
    )
    return SchemaPlan((first, second))


async def _drop_table(client: AsyncClient, table: QualifiedTable) -> None:
    await client.command(
        _DROP_TABLE.format(table=table.quoted),
        settings=dict(DDL_SETTINGS),
    )


async def _evidence_counts(
    client: AsyncClient,
    contract: NamespaceContract,
) -> tuple[int, int]:
    migration_count = await _metadata_count(
        client,
        contract,
        record_kind=MIGRATION_RECORD_KIND,
        record_key=MIGRATION_RECORD_KEY,
    )
    namespace_count = await _metadata_count(
        client,
        contract,
        record_kind=NAMESPACE_RECORD_KIND,
        record_key=contract.namespace,
    )
    return migration_count, namespace_count


async def _evidence_identity_counts(
    client: AsyncClient,
    contract: NamespaceContract,
) -> tuple[int, int]:
    migration_count = await _metadata_identity_count(
        client,
        contract,
        record_kind=MIGRATION_RECORD_KIND,
        record_key=MIGRATION_RECORD_KEY,
    )
    namespace_count = await _metadata_identity_count(
        client,
        contract,
        record_kind=NAMESPACE_RECORD_KIND,
        record_key=contract.namespace,
    )
    return migration_count, namespace_count


async def _metadata_count(
    client: AsyncClient,
    contract: NamespaceContract,
    *,
    record_kind: str,
    record_key: str,
) -> int:
    metadata_layout = MetadataLayout(contract.result_table.database)
    result = await client.query(
        _COUNT_METADATA.format(table=metadata_layout.table.quoted),
        parameters={
            "record_kind": record_kind,
            "record_key": record_key,
            "scope": contract.scope,
        },
    )
    raw_count: object = result.result_rows[0][0]
    if not isinstance(raw_count, int) or isinstance(raw_count, bool):
        msg = "metadata count query returned a non-integer"
        raise TypeError(msg)
    return raw_count


async def _metadata_identity_count(
    client: AsyncClient,
    contract: NamespaceContract,
    *,
    record_kind: str,
    record_key: str,
) -> int:
    metadata_layout = MetadataLayout(contract.result_table.database)
    result = await client.query(
        _COUNT_METADATA_IDENTITIES.format(table=metadata_layout.table.quoted),
        parameters={
            "record_kind": record_kind,
            "record_key": record_key,
            "scope": contract.scope,
        },
    )
    raw_count: object = result.result_rows[0][0]
    if not isinstance(raw_count, int) or isinstance(raw_count, bool):
        msg = "metadata identity count query returned a non-integer"
        raise TypeError(msg)
    return raw_count


def _is_namespace_insert(request: InsertRequest) -> bool:
    if not request.rows or not request.rows[0]:
        return False
    return request.rows[0][0] == NAMESPACE_RECORD_KIND


def _is_migration_insert(request: InsertRequest) -> bool:
    if not request.rows or not request.rows[0]:
        return False
    return request.rows[0][0] == MIGRATION_RECORD_KIND


def _unique_suffix(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex[:10]}"
