"""Observable production-gateway wrapper for definite write failures."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any, cast


if TYPE_CHECKING:
    from collections.abc import Mapping

    from taskiq_clickhouse._backend_runtime import BackendRuntime
    from taskiq_clickhouse._clickhouse.contracts import ReadWriteGateway
    from taskiq_clickhouse._clickhouse.request import InsertRequest
    from taskiq_clickhouse._storage.repository import StorageRepository
    from taskiq_clickhouse._types import SchemaActor, SchemaMode
    from taskiq_clickhouse.backend import ClickHouseResultBackend


@dataclass(slots=True)
class StorageGatewayProbe:
    """Count package insert and confirmation calls while delegating real I/O."""

    delegate: ReadWriteGateway
    insert_calls: int = 0
    confirmation_calls: int = 0

    async def query_rows(
        self,
        query: str,
        *,
        query_parameters: Mapping[str, object] | None = None,
        settings: Mapping[str, object] | None = None,
        column_formats: Mapping[str, str] | None = None,
    ) -> tuple[tuple[object, ...], ...]:
        """Count exact-write confirmations and forward every query."""
        if query.startswith("SELECT 1\n"):
            self.confirmation_calls += 1
        return await self.delegate.query_rows(
            query,
            query_parameters=query_parameters,
            settings=settings,
            column_formats=column_formats,
        )

    async def insert_rows(self, request: InsertRequest) -> None:
        """Count one package insert call before forwarding its real outcome."""
        self.insert_calls += 1
        await self.delegate.insert_rows(request)


@dataclass(frozen=True, slots=True, repr=False)
class _RepositoryRuntime:
    """Decorate one real runtime without forging its lifecycle state."""

    delegate: BackendRuntime
    storage: StorageRepository

    @property
    def is_new(self) -> bool:
        """Forward the real runtime's construction-state observation."""
        return self.delegate.is_new

    async def startup(self) -> None:
        """Forward startup to the owner of the real client and lease."""
        await self.delegate.startup()

    async def shutdown(self) -> None:
        """Forward terminal cleanup to the real runtime owner."""
        await self.delegate.shutdown()

    def repository(self) -> StorageRepository:
        """Enforce real READY ownership before returning the decorated store."""
        self.delegate.repository()
        return self.storage

    async def run_schema_manager(
        self,
        *,
        mode: SchemaMode,
        actor: SchemaActor,
    ) -> None:
        """Forward controlled schema work to the real runtime owner."""
        await self.delegate.run_schema_manager(mode=mode, actor=actor)


def install_storage_gateway_probe(
    backend: ClickHouseResultBackend[Any],
) -> StorageGatewayProbe:
    """Decorate the READY repository while retaining real runtime ownership."""
    runtime = backend._runtime  # noqa: SLF001 - single test composition seam.
    repository = cast("StorageRepository", runtime.repository())
    probe = StorageGatewayProbe(repository.gateway)
    decorated_runtime = _RepositoryRuntime(
        delegate=runtime,
        storage=replace(repository, gateway=probe),
    )
    backend._runtime = cast(  # noqa: SLF001 - install the same composition seam.
        "BackendRuntime",
        decorated_runtime,
    )
    return probe
