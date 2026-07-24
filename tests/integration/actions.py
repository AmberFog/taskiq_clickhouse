"""ClickHouse operations used by integration tests and fixtures."""

import asyncio
from dataclasses import asdict, dataclass
from importlib.metadata import version as distribution_version
import json
from pathlib import Path
from typing import Final

import clickhouse_connect
from clickhouse_connect.driver.asyncclient import AsyncClient
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import ClickHouseError

from tests.integration.settings import ClickHouseTestSettings


ADMIN_DATABASE: Final = "default"
CLICKHOUSE_CONNECT_DISTRIBUTION: Final = "clickhouse-connect"
CLIENT_COUNT: Final = 20
CONNECT_TIMEOUT_SECONDS: Final = 5
QUERY_TIMEOUT_SECONDS: Final = 15
CONCURRENCY_TIMEOUT_SECONDS: Final = 15

DROP_DATABASE_QUERY: Final = "DROP DATABASE IF EXISTS {database:Identifier} SYNC"
CREATE_DATABASE_QUERY: Final = "CREATE DATABASE {database:Identifier}"
ENVIRONMENT_QUERY: Final = "SELECT version(), timezone(), getSetting('async_insert')"
PARTICIPANT_QUERY: Final = "SELECT {participant:UInt8}"
DATABASE_TABLES_QUERY: Final = """
SELECT name, engine
FROM system.tables
WHERE database = {database:String}
ORDER BY name
"""
DDL_SETTINGS: Final = {"wait_end_of_query": 1}


@dataclass(frozen=True, slots=True)
class EnvironmentObservation:
    """Server values that must match the selected integration profile."""

    version: str
    timezone: str
    async_insert: int
    client_version: str


def open_admin_client(settings: ClickHouseTestSettings) -> Client:
    """Create a synchronous client for session-scoped database ownership."""
    return clickhouse_connect.get_client(
        host=settings.host,
        port=settings.port,
        username=settings.username,
        password=settings.password,
        database=ADMIN_DATABASE,
        interface="http",
        secure=False,
        connect_timeout=CONNECT_TIMEOUT_SECONDS,
        send_receive_timeout=QUERY_TIMEOUT_SECONDS,
        autogenerate_session_id=False,
        query_retries=0,
    )


async def open_async_client(settings: ClickHouseTestSettings, database: str) -> AsyncClient:
    """Create an event-loop-local client with deterministic defaults."""
    return await clickhouse_connect.get_async_client(
        host=settings.host,
        port=settings.port,
        username=settings.username,
        password=settings.password,
        database=database,
        interface="http",
        secure=False,
        connect_timeout=CONNECT_TIMEOUT_SECONDS,
        send_receive_timeout=QUERY_TIMEOUT_SECONDS,
        autogenerate_session_id=False,
        query_retries=0,
        tz_mode="aware",
    )


def recreate_database(client: Client, database: str) -> None:
    """Create a clean database owned by one pytest worker."""
    parameters = {"database": database}
    client.command(DROP_DATABASE_QUERY, parameters=parameters, settings=DDL_SETTINGS)
    client.command(CREATE_DATABASE_QUERY, parameters=parameters, settings=DDL_SETTINGS)


def drop_database(client: Client, database: str) -> None:
    """Drop the worker database synchronously."""
    client.command(
        DROP_DATABASE_QUERY,
        parameters={"database": database},
        settings=DDL_SETTINGS,
    )


async def observe_environment(client: AsyncClient) -> EnvironmentObservation:
    """Read the exact server version, timezone and default async-insert setting."""
    row = (await client.query(ENVIRONMENT_QUERY)).result_rows[0]
    return EnvironmentObservation(
        version=str(row[0]),
        timezone=str(row[1]),
        async_insert=int(row[2]),
        client_version=distribution_version(CLICKHOUSE_CONNECT_DISTRIBUTION),
    )


async def exercise_client_barrier(clients: tuple[AsyncClient, ...]) -> tuple[int, ...]:
    """Release already-created independent clients through one bounded barrier."""
    barrier = asyncio.Barrier(len(clients))
    async with asyncio.timeout(CONCURRENCY_TIMEOUT_SECONDS):
        results = await asyncio.gather(
            *(_query_after_barrier(client, participant, barrier) for participant, client in enumerate(clients)),
        )
    return tuple(results)


async def write_environment_evidence(
    settings: ClickHouseTestSettings,
    database: str,
    observation: EnvironmentObservation,
) -> None:
    """Persist non-secret server evidence outside the event-loop thread."""
    payload = {
        **asdict(observation),
        "database": database,
        "profile": settings.profile,
    }
    evidence_path = settings.evidence_dir / "pytest-environment.json"
    await asyncio.to_thread(_write_json, evidence_path, payload)


def write_database_evidence(
    client: Client,
    settings: ClickHouseTestSettings,
    database: str,
) -> None:
    """Persist non-secret table evidence before deterministic teardown."""
    try:
        rows = client.query(
            DATABASE_TABLES_QUERY,
            parameters={"database": database},
        ).result_rows
        tables = tuple((str(name), str(engine)) for name, engine in rows)
        payload: dict[str, object] = {"database": database, "tables": tables}
    except ClickHouseError:  # Evidence collection must not prevent database cleanup.
        payload = {"database": database, "collection_error": "query-failed"}
    _write_json(settings.evidence_dir / "pytest-database.json", payload)


async def _query_after_barrier(
    client: AsyncClient,
    participant: int,
    barrier: asyncio.Barrier,
) -> int:
    await barrier.wait()
    row = (
        await client.query(
            PARTICIPANT_QUERY,
            parameters={"participant": participant},
        )
    ).result_rows[0]
    return int(row[0])


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
