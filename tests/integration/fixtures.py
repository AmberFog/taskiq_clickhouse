"""Loop-safe pytest fixtures for the real ClickHouse service."""

from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from contextlib import AsyncExitStack
from typing import TypeAlias

from aiohttp import ClientError
from clickhouse_connect.driver.asyncclient import AsyncClient
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import ClickHouseError
import pytest
import pytest_asyncio

from tests.integration.actions import (
    CLIENT_COUNT,
    drop_database,
    open_admin_client,
    open_async_client,
    recreate_database,
    write_database_evidence,
)
from tests.integration.settings import (
    ClickHouseTestSettings,
    load_clickhouse_settings,
    make_worker_database_name,
)


ClickHouseClientFactory: TypeAlias = Callable[[], Awaitable[AsyncClient]]


@pytest.fixture(scope="session")
def clickhouse_settings() -> ClickHouseTestSettings:
    """Load the explicit service contract or fail required integration tests."""
    try:
        settings = load_clickhouse_settings()
    except ValueError as error:
        pytest.fail(str(error), pytrace=False)
    settings.evidence_dir.mkdir(parents=True, exist_ok=True)
    return settings


@pytest.fixture(scope="session")
def clickhouse_database(
    clickhouse_settings: ClickHouseTestSettings,
) -> Iterator[str]:
    """Own one collision-resistant database for the current pytest worker."""
    database = make_worker_database_name(clickhouse_settings)
    admin_client = _open_required_admin(clickhouse_settings)
    try:
        recreate_database(admin_client, database)
    except (ClickHouseError, OSError):
        _close_after_setup_failure(admin_client)

    yield database

    failures = _teardown_worker_database(
        admin_client,
        clickhouse_settings,
        database,
    )
    if failures:
        pytest.fail(
            f"required ClickHouse database cleanup failed: {', '.join(failures)}",
            pytrace=False,
        )


@pytest_asyncio.fixture
async def clickhouse_client_factory(
    clickhouse_settings: ClickHouseTestSettings,
    clickhouse_database: str,
) -> AsyncIterator[ClickHouseClientFactory]:
    """Yield a loop-local client factory that closes every returned client."""
    try:
        async with AsyncExitStack() as client_stack:

            async def create_client() -> AsyncClient:
                client = await open_async_client(clickhouse_settings, clickhouse_database)
                client_stack.push_async_callback(client.close)
                return client

            yield create_client
    except (ClickHouseError, ClientError, OSError):
        pytest.fail("required ClickHouse client setup or teardown failed", pytrace=False)


@pytest_asyncio.fixture
async def clickhouse_client(
    clickhouse_client_factory: ClickHouseClientFactory,
) -> AsyncIterator[AsyncClient]:
    """Yield one client owned by the current test's client factory."""
    yield await clickhouse_client_factory()


@pytest_asyncio.fixture
async def clickhouse_clients(
    clickhouse_client_factory: ClickHouseClientFactory,
) -> AsyncIterator[tuple[AsyncClient, ...]]:
    """Create twenty independent clients before exposing the barrier fixture."""
    clients = tuple([await clickhouse_client_factory() for _client_number in range(CLIENT_COUNT)])
    yield clients


def _open_required_admin(settings: ClickHouseTestSettings) -> Client:
    try:
        return open_admin_client(settings)
    except (ClickHouseError, OSError):
        pytest.fail("required ClickHouse service is unavailable or unhealthy", pytrace=False)


def _close_after_setup_failure(client: Client) -> None:
    cleanup_failed = False
    try:
        client.close()
    except (ClickHouseError, OSError):
        cleanup_failed = True
    message = "required ClickHouse database setup failed"
    if cleanup_failed:
        message = f"{message}; client cleanup also failed"
    pytest.fail(message, pytrace=False)


def _teardown_worker_database(
    client: Client,
    settings: ClickHouseTestSettings,
    database: str,
) -> tuple[str, ...]:
    failures: list[str] = []
    try:
        write_database_evidence(client, settings, database)
    except (ClickHouseError, OSError):
        failures.append("evidence")
    try:
        drop_database(client, database)
    except (ClickHouseError, OSError):
        failures.append("drop")
    try:
        client.close()
    except (ClickHouseError, OSError):
        failures.append("close")
    return tuple(failures)
