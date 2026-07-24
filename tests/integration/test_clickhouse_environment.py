"""Verify the explicitly configured real ClickHouse environment."""

from clickhouse_connect.driver.asyncclient import AsyncClient
import pytest

from tests.integration.actions import (
    CLIENT_COUNT,
    exercise_client_barrier,
    observe_environment,
    write_environment_evidence,
)
from tests.integration.settings import ClickHouseTestSettings


pytestmark = [pytest.mark.clickhouse, pytest.mark.asyncio]


async def test_server_profile_matches_expectations(
    clickhouse_client: AsyncClient,
    clickhouse_database: str,
    clickhouse_settings: ClickHouseTestSettings,
) -> None:
    """Require the exact selected image, UTC and async-insert default."""
    observation = await observe_environment(clickhouse_client)

    assert observation.version == clickhouse_settings.expected_version
    assert observation.timezone == "UTC"
    assert observation.async_insert == clickhouse_settings.expected_async_insert
    assert observation.client_version == clickhouse_settings.expected_client_version
    await write_environment_evidence(
        clickhouse_settings,
        clickhouse_database,
        observation,
    )


async def test_twenty_independent_clients_cross_one_barrier(
    clickhouse_clients: tuple[AsyncClient, ...],
) -> None:
    """Prove later concurrent-startup tests have enough isolated clients."""
    assert len(clickhouse_clients) == CLIENT_COUNT
    assert len({id(client) for client in clickhouse_clients}) == CLIENT_COUNT

    results = await exercise_client_barrier(clickhouse_clients)

    assert results == tuple(range(CLIENT_COUNT))
