"""Public backend construction for isolated ClickHouse failure scenarios."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from taskiq_clickhouse._storage.layout import (
    StorageLayout,
    storage_layout_from_names,
)
from taskiq_clickhouse.backend import ClickHouseResultBackend
from taskiq_clickhouse.schema import ClickHouseSchemaManager


if TYPE_CHECKING:
    from taskiq_clickhouse._types import SchemaMode
    from tests.integration.settings import ClickHouseTestSettings


_RESULT_TTL = timedelta(hours=1)
_PURGE_TTL = timedelta(days=1)


@dataclass(frozen=True, slots=True)
class BackendScope:
    """One unique namespace and table pair shared by cooperating actors."""

    namespace: str
    result_table: str
    progress_table: str

    @classmethod
    def unique(cls, prefix: str) -> BackendScope:
        """Create one collision-resistant scope for a single test scenario."""
        suffix = f"{prefix}_{uuid4().hex[:12]}"
        return cls(
            namespace=f"failure-{suffix}",
            result_table=f"result_{suffix}",
            progress_table=f"progress_{suffix}",
        )

    def storage_layout(self, database: str) -> StorageLayout:
        """Return the production storage layout for this scope."""
        return storage_layout_from_names(
            database,
            self.result_table,
            self.progress_table,
        )


@dataclass(frozen=True, slots=True, repr=False)
class BackendCredentials:
    """Connection identity whose password must never appear in diagnostics."""

    username: str | None
    password: str = field(repr=False)


def build_backend(
    settings: ClickHouseTestSettings,
    database: str,
    scope: BackendScope,
    credentials: BackendCredentials,
    *,
    schema_mode: SchemaMode,
) -> ClickHouseResultBackend[Any]:
    """Build a public backend with no construction-time network access."""
    return ClickHouseResultBackend(
        host=_backend_host(settings),
        port=settings.port,
        username=credentials.username,
        password=credentials.password,
        database=database,
        secure=False,
        result_ttl=_RESULT_TTL,
        purge_ttl=_PURGE_TTL,
        namespace=scope.namespace,
        result_table=scope.result_table,
        progress_table=scope.progress_table,
        keep_results=True,
        schema_mode=schema_mode,
    )


async def preprovision_scope(
    settings: ClickHouseTestSettings,
    database: str,
    scope: BackendScope,
) -> None:
    """Install and register one scope with the integration administrator."""
    credentials = BackendCredentials(settings.username, settings.password)
    backend = build_backend(
        settings,
        database,
        scope,
        credentials,
        schema_mode="validate",
    )
    try:
        await ClickHouseSchemaManager(backend).migrate()
    finally:
        await backend.shutdown()


def endpoint_sentinels(settings: ClickHouseTestSettings) -> tuple[str, ...]:
    """Return exact private endpoint forms that public errors must redact."""
    host = _backend_host(settings)
    authority = f"{host}:{settings.port}"
    return authority, f"http://{authority}"


def _backend_host(settings: ClickHouseTestSettings) -> str:
    return "localhost" if ":" in settings.host else settings.host
