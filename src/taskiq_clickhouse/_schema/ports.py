"""Narrow schema-policy ports implemented by ClickHouse adapters."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol


if TYPE_CHECKING:
    from taskiq_clickhouse._schema.contracts import SchemaContract
    from taskiq_clickhouse._schema.integrity import MigrationHistory
    from taskiq_clickhouse._schema.migrations import MigrationDefinition, SchemaPlan
    from taskiq_clickhouse._types import SchemaMode


class SchemaVerifier(Protocol):
    """Inspect only the physical postconditions needed by schema policy."""

    async def matches(self, contract: SchemaContract) -> bool:
        """Return whether the complete physical contract currently holds."""
        ...

    async def validate(self, contract: SchemaContract) -> None:
        """Raise a typed drift error when the contract does not hold."""
        ...


class MigrationRegistry(Protocol):
    """Read and append only the migration evidence needed by execution."""

    async def read_history(self, plan: SchemaPlan) -> MigrationHistory:
        """Return validated evidence for the plan's exact scope."""
        ...

    async def record_migration(self, migration: MigrationDefinition) -> None:
        """Append acknowledged evidence for a physically verified migration."""
        ...


class SchemaRegistry(MigrationRegistry, Protocol):
    """Coordinate metadata bootstrap and namespace evidence at readiness."""

    async def validate_retention(self) -> None:
        """Check retention against the authoritative clock without writing."""
        ...

    async def bootstrap(self, verifier: SchemaVerifier, *, mode: SchemaMode) -> None:
        """Create when allowed, then verify the physical registry."""
        ...

    async def ensure_namespace(self, *, mode: SchemaMode) -> None:
        """Validate or register the immutable namespace contract."""
        ...
