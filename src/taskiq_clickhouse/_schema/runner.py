"""Fail-closed schema migration and namespace readiness barrier."""

from __future__ import annotations

from dataclasses import dataclass
from importlib.metadata import version as distribution_version
from typing import TYPE_CHECKING

from taskiq_clickhouse._clickhouse import (
    adapter as clickhouse_adapter,
    contracts as clickhouse_contracts,
    errors as clickhouse_errors,
)
from taskiq_clickhouse._schema import (
    _inspection_types as inspection_types,
    inspection,
    layout,
    ports,
    registry,
)
from taskiq_clickhouse._types import MigrationExecution, SchemaActor
from taskiq_clickhouse._write_acknowledgement import AttemptOutcome
from taskiq_clickhouse.exceptions import (
    ClickHouseBackendIOError,
    ClickHouseMigrationError,
    ClickHouseResultBackendError,
    ClickHouseSchemaDriftError,
    _PhysicalSchemaDriftError,
)


if TYPE_CHECKING:
    from collections.abc import Mapping

    from clickhouse_connect.driver.asyncclient import AsyncClient

    from taskiq_clickhouse._schema.migrations import MigrationDefinition, MigrationStep, SchemaPlan
    from taskiq_clickhouse._schema.records import NamespaceContract
    from taskiq_clickhouse._schema_drift import SchemaDriftReport
    from taskiq_clickhouse._types import SchemaMode


@dataclass(frozen=True, slots=True)
class SchemaBarrierContext:
    """Immutable schema policy needed by every temporary client barrier."""

    namespace_contract: NamespaceContract
    plan: SchemaPlan
    package_version: str

    @classmethod
    def production(
        cls,
        namespace_contract: NamespaceContract,
        plan: SchemaPlan,
    ) -> SchemaBarrierContext:
        """Compose a context with the installed distribution identity."""
        return cls(
            namespace_contract=namespace_contract,
            plan=plan,
            package_version=distribution_version("taskiq-clickhouse"),
        )

    def __post_init__(self) -> None:
        """Reject incomplete package identity before any schema I/O."""
        if not isinstance(self.package_version, str) or not self.package_version:
            msg = "package_version must be a non-empty string"
            raise ValueError(msg)


@dataclass(frozen=True, slots=True)
class _DriftFailure:
    """Traceback-free schema drift retained across a recovery read."""

    operation: str
    reason: str
    report: SchemaDriftReport | None

    @classmethod
    def capture(cls, error: ClickHouseSchemaDriftError) -> _DriftFailure:
        """Freeze safe public codes and an optional value-free report."""
        report = error.report if type(error) is _PhysicalSchemaDriftError else None  # noqa: WPS516 - exact private carrier is the only report source.
        return cls(error.operation, error.reason, report)

    def rebuild(self) -> ClickHouseSchemaDriftError:
        """Create a fresh error without retaining the original traceback."""
        if self.report is not None:
            return _PhysicalSchemaDriftError(self.report)
        return ClickHouseSchemaDriftError(self.operation, self.reason)


@dataclass(frozen=True, slots=True)
class _MigrationExecutor:
    """Apply one immutable definition through before/after postconditions."""

    gateway: clickhouse_contracts.CommandExecutor
    inspector: ports.SchemaVerifier
    registry: ports.MigrationRegistry

    async def apply(self, migration: MigrationDefinition) -> None:
        """Recover completed steps and record success only after validation."""
        for step in migration.steps:
            await self._apply_step(step)  # noqa: WPS476  # Migration DDL is strictly ordered.
        await self.inspector.validate(migration.target)
        await self.registry.record_migration(migration)

    async def capture_drift(
        self,
        migration: MigrationDefinition,
    ) -> _DriftFailure | None:
        """Return a safe drift only after leaving its active exception context."""
        try:
            await self.apply(migration)
        except ClickHouseSchemaDriftError as error:
            return _DriftFailure.capture(error)
        return None

    async def recover_version(
        self,
        migration: MigrationDefinition,
        plan: SchemaPlan,
        drift_failure: _DriftFailure,
    ) -> int:
        """Resolve obsolete drift only through newer durable history."""
        history = await self.registry.read_history(plan)
        if history.applied_version < migration.version:
            raise drift_failure.rebuild() from None
        return history.applied_version

    def require_allowed(
        self,
        migration: MigrationDefinition,
        *,
        actor: SchemaActor,
    ) -> None:
        """Enforce the worker/manager execution policy before DDL."""
        if actor is SchemaActor.MANAGER:
            return
        if migration.execution is not MigrationExecution.AUTO:
            operation = "migration_policy"
            reason = "controlled_pending"
            raise ClickHouseMigrationError(operation, reason) from None

    async def _apply_step(self, step: MigrationStep) -> None:
        if not await self._requires_execution(step):
            return
        outcome = await self._execute_ddl(step.ddl, step.query_parameters)
        if outcome is AttemptOutcome.AMBIGUOUS:
            after_matches = await self.inspector.matches(step.after)
            if not after_matches:
                operation = "migration_execute"
                reason = "ddl_unconfirmed"
                raise ClickHouseBackendIOError(operation, reason) from None
        await self.inspector.validate(step.after)

    async def _requires_execution(self, step: MigrationStep) -> bool:
        """Resolve a concurrent DDL win before declaring partial state."""
        if await self.inspector.matches(step.after):
            return False
        if await self.inspector.matches(step.before):
            return True
        if await self.inspector.matches(step.after):
            return False
        operation = "migration_execute"
        reason = "partial_state"
        raise ClickHouseSchemaDriftError(operation, reason) from None

    async def _execute_ddl(
        self,
        ddl: str,
        query_parameters: Mapping[str, str],
    ) -> AttemptOutcome:
        error_reason: str | None = None
        try:
            await self.gateway.command(
                ddl,
                query_parameters=query_parameters,
                settings=layout.DDL_SETTINGS,
            )
        except clickhouse_errors.AmbiguousClickHouseError:
            return AttemptOutcome.AMBIGUOUS
        except clickhouse_errors.DefiniteClickHouseError:
            error_reason = "database_error"
        if error_reason is not None:
            operation = "migration_execute"
            raise ClickHouseMigrationError(operation, error_reason) from None
        return AttemptOutcome.ACKNOWLEDGED


@dataclass(frozen=True, slots=True)
class SchemaRunner:
    """Execute the complete migration, physical and namespace barrier."""

    gateway: clickhouse_contracts.CommandExecutor
    inspector: ports.SchemaVerifier
    registry: ports.SchemaRegistry
    plan: SchemaPlan

    async def run(self, *, mode: SchemaMode, actor: SchemaActor) -> None:
        """Return only after two complete, conflict-free final reads."""
        if mode == "migrate":
            await self.registry.validate_retention()
        await self.registry.bootstrap(self.inspector, mode=mode)
        history = await self.registry.read_history(self.plan)
        if mode == "validate":
            self._require_target(history.applied_version)
        else:
            await self._migrate_from(history.applied_version, actor=actor)
        await self._cross_target_barrier(mode=mode)

    async def _cross_target_barrier(self, *, mode: SchemaMode) -> None:
        """Perform the namespace step and a second complete final read."""
        await self._validate_physical()
        await self.registry.ensure_namespace(mode=mode)
        final_history = await self.registry.read_history(self.plan)
        self._require_target(final_history.applied_version)
        await self._validate_physical()
        await self.registry.ensure_namespace(mode="validate")

    async def _migrate_from(self, applied_version: int, *, actor: SchemaActor) -> None:
        executor = _MigrationExecutor(self.gateway, self.inspector, self.registry)
        observed_version = applied_version
        while observed_version < self.plan.target_version:
            observed_version = await self._advance_one(  # noqa: WPS476  # Versions are strictly ordered.
                executor,
                observed_version=observed_version,
                actor=actor,
            )

    async def _advance_one(
        self,
        executor: _MigrationExecutor,
        *,
        observed_version: int,
        actor: SchemaActor,
    ) -> int:
        history = await self.registry.read_history(self.plan)
        if history.applied_version != observed_version:
            if history.applied_version < observed_version:
                operation = "migration_barrier"
                reason = "history_not_advanced"
                raise ClickHouseMigrationError(operation, reason) from None
            return history.applied_version
        migration = self.plan.migrations[observed_version]
        executor.require_allowed(migration, actor=actor)
        drift_failure = await executor.capture_drift(migration)
        if drift_failure is not None:
            return await executor.recover_version(migration, self.plan, drift_failure)
        return await self._read_recorded_version(migration.version)

    async def _read_recorded_version(self, migration_version: int) -> int:
        history = await self.registry.read_history(self.plan)
        if history.applied_version < migration_version:
            operation = "migration_barrier"
            reason = "history_not_advanced"
            raise ClickHouseMigrationError(operation, reason) from None
        return history.applied_version

    async def _validate_physical(self) -> None:
        await self.registry.bootstrap(self.inspector, mode="validate")
        if self.plan.migrations:
            target = self.plan.migrations[-1].target
            await self.inspector.validate(target)

    def _require_target(self, applied_version: int) -> None:
        if applied_version != self.plan.target_version:
            operation = "migration_barrier"
            reason = "migration_missing"
            raise ClickHouseMigrationError(operation, reason) from None


async def run_schema_barrier(
    context: SchemaBarrierContext,
    client: AsyncClient,
    *,
    mode: SchemaMode,
    actor: SchemaActor,
) -> None:
    """Build and execute the package barrier for one temporary client."""
    failure_reason: str | None = None
    try:
        await _run_context_barrier(
            context,
            client,
            mode=mode,
            actor=actor,
        )
    except ClickHouseResultBackendError:
        raise
    except inspection_types.SchemaInspectionError:
        failure_reason = "invalid_response"
    if failure_reason is not None:
        operation = "schema_barrier"
        raise ClickHouseBackendIOError(operation, failure_reason) from None


async def _run_context_barrier(
    context: SchemaBarrierContext,
    client: AsyncClient,
    *,
    mode: SchemaMode,
    actor: SchemaActor,
) -> None:
    schema_runner = _build_runner(context, client)
    await schema_runner.run(mode=mode, actor=actor)


def _build_runner(
    context: SchemaBarrierContext,
    client: AsyncClient,
) -> SchemaRunner:
    schema_gateway = clickhouse_adapter.ClickHouseGateway(client)
    metadata_registry = registry.MetadataRegistry(
        gateway=schema_gateway,
        namespace_contract=context.namespace_contract,
        package_version=context.package_version,
    )
    return SchemaRunner(
        gateway=schema_gateway,
        inspector=inspection.SchemaInspector(schema_gateway),
        registry=metadata_registry,
        plan=context.plan,
    )
