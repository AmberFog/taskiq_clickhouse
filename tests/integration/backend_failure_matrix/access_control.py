"""Test-owned ClickHouse users and exact least-privilege grants."""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Final
from uuid import uuid4

from taskiq_clickhouse._identifiers import Identifier, QualifiedTable
from taskiq_clickhouse._schema.layout import DDL_SETTINGS, MetadataLayout


if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from clickhouse_connect.driver.asyncclient import AsyncClient

    from tests.integration.backend_failure_matrix.backend_factory import BackendScope


_SYSTEM = Identifier("system")
_SYSTEM_ONE: Final = QualifiedTable(_SYSTEM, Identifier("one"))
_SYSTEM_TABLES: Final = QualifiedTable(_SYSTEM, Identifier("tables"))
_SYSTEM_COLUMNS: Final = QualifiedTable(_SYSTEM, Identifier("columns"))
_SYSTEM_DATA_SKIPPING_INDICES: Final = QualifiedTable(
    _SYSTEM,
    Identifier("data_skipping_indices"),
)
_SYSTEM_PROJECTIONS: Final = QualifiedTable(_SYSTEM, Identifier("projections"))
_CREATE_USER: Final = "CREATE USER {username} IDENTIFIED WITH sha256_password BY {{password:String}}"
_DROP_USER: Final = "DROP USER IF EXISTS {username}"


@dataclass(frozen=True, slots=True)
class ManagedUser:
    """One globally unique ClickHouse user removed after its scenario."""

    username: Identifier
    password: str = field(repr=False)


@asynccontextmanager
async def managed_user(
    client: AsyncClient,
    *,
    prefix: str,
    password: str,
) -> AsyncIterator[ManagedUser]:
    """Create one password user and always remove its grants and identity."""
    username = Identifier(f"{prefix}_{uuid4().hex[:12]}")
    user = ManagedUser(username, password)
    await client.command(
        _CREATE_USER.format(username=username.quoted),
        parameters={"password": password},
        settings=dict(DDL_SETTINGS),
    )
    try:
        yield user
    finally:
        await client.command(
            _DROP_USER.format(username=username.quoted),
            settings=dict(DDL_SETTINGS),
        )


async def grant_connectivity_only(client: AsyncClient, user: ManagedUser) -> None:
    """Allow a basic query while deliberately withholding every schema read."""
    await _grant(client, user.username, "SELECT", _SYSTEM_ONE.quoted)


async def grant_validate_and_data_reads(
    client: AsyncClient,
    user: ManagedUser,
    database: str,
    scope: BackendScope,
) -> None:
    """Grant startup validation and data reads without any INSERT or DDL."""
    storage = scope.storage_layout(database)
    metadata = MetadataLayout(storage.database).table
    grants = (
        ("SELECT", _SYSTEM_ONE.quoted),
        ("SELECT", _SYSTEM_TABLES.quoted),
        ("SELECT", _SYSTEM_COLUMNS.quoted),
        ("SELECT", _SYSTEM_DATA_SKIPPING_INDICES.quoted),
        ("SELECT", _SYSTEM_PROJECTIONS.quoted),
        ("SELECT", metadata.quoted),
        ("SELECT", storage.result_table.quoted),
        ("SELECT", storage.progress_table.quoted),
        ("SHOW COLUMNS", f"{storage.database.quoted}.*"),
    )
    for privilege, target in grants:
        await _grant(client, user.username, privilege, target)


async def revoke_result_select(
    client: AsyncClient,
    user: ManagedUser,
    database: str,
    scope: BackendScope,
) -> None:
    """Remove only the result-table read needed by public readiness."""
    result_table = scope.storage_layout(database).result_table
    query = f"REVOKE SELECT ON {result_table.quoted} FROM {user.username.quoted}"  # noqa: S608
    await client.command(query, settings=dict(DDL_SETTINGS))


async def _grant(
    client: AsyncClient,
    username: Identifier,
    privilege: str,
    target: str,
) -> None:
    query = f"GRANT {privilege} ON {target} TO {username.quoted}"
    await client.command(query, settings=dict(DDL_SETTINGS))
