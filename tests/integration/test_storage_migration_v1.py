"""Exercise production migration v1 against real ClickHouse."""

import asyncio
from typing import Final

from clickhouse_connect.driver.asyncclient import AsyncClient
import pytest

from taskiq_clickhouse._clickhouse.adapter import ClickHouseGateway
from taskiq_clickhouse._schema.inspection import SchemaInspector
from taskiq_clickhouse._schema.layout import DDL_SETTINGS
from taskiq_clickhouse._storage.layout import build_storage_plan
from taskiq_clickhouse._types import SchemaActor
from taskiq_clickhouse.exceptions import ClickHouseSchemaDriftError
from taskiq_clickhouse.schema import ClickHouseSchemaManager
from tests.integration.settings import ClickHouseTestSettings
from tests.integration.storage_migration_contract import (
    actions,
    assertions,
    cases,
    probes,
)
from tests.integration.storage_migration_contract.gateways import MigrationGatewayProbe


pytestmark = [pytest.mark.clickhouse, pytest.mark.asyncio]

_CLIENT_COUNT: Final = 20
_MIGRATION_STEP_COUNT: Final = 2
_CONCURRENCY_TIMEOUT_SECONDS: Final = 20


async def test_public_manager_migrates_validates_and_restarts_without_growth(
    clickhouse_client: AsyncClient,
    clickhouse_database: str,
    clickhouse_settings: ClickHouseTestSettings,
) -> None:
    """Create production tables from absence and prove exact idempotency."""
    async with actions.isolated_migration_namespace(
        clickhouse_client,
        clickhouse_database,
        "public_v1",
    ) as namespace:
        await ClickHouseSchemaManager(
            actions.build_backend(clickhouse_settings, namespace),
        ).migrate()
        initial_evidence = await probes.metadata_evidence(clickhouse_client, namespace.contract)

        await ClickHouseSchemaManager(
            actions.build_backend(clickhouse_settings, namespace),
        ).validate()
        await ClickHouseSchemaManager(
            actions.build_backend(clickhouse_settings, namespace),
        ).migrate()

        assertions.assert_metadata_record_bounds(initial_evidence[0], max_identities=1)
        assertions.assert_metadata_record_bounds(initial_evidence[1], max_identities=1)
        assert await probes.metadata_evidence(clickhouse_client, namespace.contract) == initial_evidence
        await assertions.assert_physical_contract(clickhouse_client, namespace.layout)


async def test_twenty_clients_converge_on_exact_two_step_production_v1(
    clickhouse_clients: tuple[AsyncClient, ...],
    clickhouse_database: str,
) -> None:
    """Converge twenty independent clients on both production DDL steps."""
    async with actions.isolated_migration_namespace(
        clickhouse_clients[0],
        clickhouse_database,
        "concurrent_v1",
    ) as namespace:
        plan = build_storage_plan(namespace.layout)
        barrier = asyncio.Barrier(len(clickhouse_clients))

        async def apply(client: AsyncClient) -> None:
            await barrier.wait()
            await actions.build_schema_runner(ClickHouseGateway(client), namespace).run(
                mode="migrate",
                actor=SchemaActor.WORKER,
            )

        assert len(clickhouse_clients) == _CLIENT_COUNT
        assert len(plan.migrations[0].steps) == _MIGRATION_STEP_COUNT
        async with asyncio.timeout(_CONCURRENCY_TIMEOUT_SECONDS):
            await asyncio.gather(*(apply(client) for client in clickhouse_clients))
        initial_evidence = await probes.metadata_evidence(clickhouse_clients[0], namespace.contract)

        await actions.build_schema_runner(
            ClickHouseGateway(clickhouse_clients[0]),
            namespace,
        ).run(mode="migrate", actor=SchemaActor.WORKER)

        migration_evidence, namespace_evidence = initial_evidence
        assertions.assert_metadata_record_bounds(migration_evidence, max_identities=_CLIENT_COUNT)
        assertions.assert_metadata_record_bounds(namespace_evidence, max_identities=_CLIENT_COUNT)
        assert await probes.metadata_evidence(clickhouse_clients[0], namespace.contract) == initial_evidence
        await assertions.assert_physical_contract(clickhouse_clients[0], namespace.layout)


@pytest.mark.parametrize("scenario", cases.RESPONSE_LOSS_CASES)
async def test_production_v1_recovers_each_committed_response_loss(
    scenario: cases.ResponseLossScenario,
    clickhouse_client: AsyncClient,
    clickhouse_database: str,
) -> None:
    """Recover result DDL, progress DDL and exact history response losses."""
    async with actions.isolated_migration_namespace(
        clickhouse_client,
        clickhouse_database,
        f"loss_{scenario.response.value}",
    ) as namespace:
        probe = MigrationGatewayProbe(
            delegate=ClickHouseGateway(clickhouse_client),
            layout=namespace.layout,
            response_loss=scenario,
        )

        await actions.build_schema_runner(probe, namespace).run(
            mode="migrate",
            actor=SchemaActor.WORKER,
        )

        assert probe.loss_count == 1
        assert probe.confirmation_count == scenario.expected_confirmation_count
        migration_evidence, namespace_evidence = await probes.metadata_evidence(
            clickhouse_client,
            namespace.contract,
        )
        assertions.assert_metadata_record_bounds(migration_evidence, max_identities=1)
        assertions.assert_metadata_record_bounds(namespace_evidence, max_identities=1)
        await assertions.assert_physical_contract(clickhouse_client, namespace.layout)


@pytest.mark.parametrize("scenario", cases.PHYSICAL_DRIFT_CASES)
async def test_forged_current_history_with_physical_drift_fails_before_insert(
    scenario: cases.PhysicalDriftScenario,
    clickhouse_client: AsyncClient,
    clickhouse_database: str,
) -> None:
    """Keep forged history from masking any owned physical-schema drift."""
    async with actions.isolated_migration_namespace(
        clickhouse_client,
        clickhouse_database,
        f"forged_{scenario.case_id.replace('-', '_')}",
    ) as namespace:
        plan = build_storage_plan(namespace.layout)
        setup_gateway = ClickHouseGateway(clickhouse_client)
        setup_registry = actions.build_metadata_registry(setup_gateway, namespace)
        await setup_registry.bootstrap(SchemaInspector(setup_gateway), mode="migrate")
        await setup_registry.record_migration(plan.migrations[0])
        await actions.install_result_table_drift(setup_gateway, namespace.layout, scenario)
        if scenario.creates_dependent_view:
            await assertions.assert_dependent_view_rejects_insert(setup_gateway, namespace.layout)
        await setup_gateway.command(
            namespace.layout.create_progress_query,
            query_parameters=namespace.layout.create_progress_parameters,
            settings=DDL_SETTINGS,
        )
        await assertions.assert_result_table_drift_installed(
            setup_gateway,
            namespace.layout,
            expected_path=scenario.expected_mismatch_path,
            expected_actual=scenario.expected_actual,
        )
        probe = MigrationGatewayProbe(
            delegate=setup_gateway,
            layout=namespace.layout,
        )

        with pytest.raises(ClickHouseSchemaDriftError):
            await actions.build_schema_runner(probe, namespace).run(
                mode="migrate",
                actor=SchemaActor.WORKER,
            )

        assert probe.insert_count == 0
        assert await probes.storage_row_counts(clickhouse_client, namespace.layout) == (0, 0)
        migration_evidence, namespace_evidence = await probes.metadata_evidence(
            clickhouse_client,
            namespace.contract,
        )
        assertions.assert_metadata_record_bounds(migration_evidence, max_identities=1)
        assert namespace_evidence == (0, 0)
