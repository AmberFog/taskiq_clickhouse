"""Public controlled schema-management surface."""

__all__ = ("ClickHouseSchemaManager",)

from typing import Any

from taskiq_clickhouse._public_error_boundary import (
    detach_public_errors,
    detach_public_errors_async,
)
from taskiq_clickhouse._types import SchemaActor
from taskiq_clickhouse.backend import ClickHouseResultBackend, _run_schema_manager
from taskiq_clickhouse.exceptions import ClickHouseLifecycleError


class ClickHouseSchemaManager:
    """Run one controlled schema operation on a NEW backend instance."""

    @detach_public_errors
    def __init__(self, backend: ClickHouseResultBackend[Any]) -> None:
        """Retain a typed backend without creating clients or acquiring locks."""
        self._backend = _require_backend(backend)
        self._used = False

    @detach_public_errors_async
    async def migrate(self) -> None:
        """Apply AUTO and operator-controlled migrations once."""
        self._claim()
        await _run_schema_manager(
            self._backend,
            mode="migrate",
            actor=SchemaActor.MANAGER,
        )

    @detach_public_errors_async
    async def validate(self) -> None:
        """Validate the complete schema barrier without issuing writes."""
        self._claim()
        await _run_schema_manager(
            self._backend,
            mode="validate",
            actor=SchemaActor.WORKER,
        )

    def _claim(self) -> None:
        if self._used:
            raise _already_used_error()
        self._used = True


def _already_used_error() -> ClickHouseLifecycleError:
    return ClickHouseLifecycleError("schema_manager", "already_used")


def _require_backend(candidate: object) -> ClickHouseResultBackend[Any]:
    if not isinstance(candidate, ClickHouseResultBackend):
        message = "backend must be a ClickHouseResultBackend instance"
        raise TypeError(message)
    return candidate
