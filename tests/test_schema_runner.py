"""Unit tests for the fail-closed migration and readiness barrier."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, cast

import pytest

from taskiq_clickhouse._clickhouse.errors import (
    AmbiguousClickHouseError,
    DefiniteClickHouseError,
)
from taskiq_clickhouse._identifiers import Identifier, QualifiedTable
from taskiq_clickhouse._schema import runner as runner_module
from taskiq_clickhouse._schema._inspection_types import (
    SchemaInspectionError,
)
from taskiq_clickhouse._schema.contracts import ColumnContract, SchemaContract, TableContract
from taskiq_clickhouse._schema.integrity import MigrationHistory
from taskiq_clickhouse._schema.migrations import (
    MigrationDefinition,
    MigrationStep,
    SchemaPlan,
)
from taskiq_clickhouse._schema.registry import MetadataRegistry
from taskiq_clickhouse._schema.runner import SchemaBarrierContext, SchemaRunner
from taskiq_clickhouse._schema_drift import SchemaDriftLocation, SchemaDriftReport
from taskiq_clickhouse._types import MigrationExecution, SchemaActor
from taskiq_clickhouse.exceptions import (
    ClickHouseBackendIOError,
    ClickHouseMigrationError,
    ClickHouseNamespaceError,
    ClickHouseSchemaDriftError,
    _PhysicalSchemaDriftError,
)
from tests.schema_testkit import ScriptedGateway, namespace_contract, synthetic_plan


if TYPE_CHECKING:
    from collections.abc import Callable

    from clickhouse_connect.driver.asyncclient import AsyncClient

    from taskiq_clickhouse._types import SchemaMode


_METADATA_VALIDATIONS_PER_RUN = 3


@dataclass(slots=True)
class _StateInspector:
    current: SchemaContract
    validated: list[SchemaContract] = field(default_factory=list)
    matched: list[SchemaContract] = field(default_factory=list)
    match_results: list[bool] = field(default_factory=list)
    match_event: Callable[[], None] | None = None
    validation_error: ClickHouseSchemaDriftError | None = None

    async def matches(self, contract: SchemaContract) -> bool:
        self.matched.append(contract)
        event = self.match_event
        self.match_event = None
        if event is not None:
            event()
        if self.match_results:
            return self.match_results.pop(0)
        return self.current == contract

    async def validate(self, contract: SchemaContract) -> None:
        self.validated.append(contract)
        if self.validation_error is not None:
            raise self.validation_error
        if self.current != contract:
            operation = "schema_validation"
            reason = "physical_drift"
            raise ClickHouseSchemaDriftError(operation, reason)


@dataclass(slots=True)
class _StateRegistry:
    applied_version: int
    advance_on_record: bool = True
    retention_validations: int = 0
    retention_error: BaseException | None = None
    bootstrap_modes: list[SchemaMode] = field(default_factory=list)
    history_reads: int = 0
    recorded: list[MigrationDefinition] = field(default_factory=list)
    namespace_modes: list[SchemaMode] = field(default_factory=list)
    record_event: Callable[[_StateRegistry, MigrationDefinition], None] | None = None
    history_versions: list[int] = field(default_factory=list)
    history_error_at: int | None = None
    history_error: BaseException | None = None
    layout_validations: int = 0
    layout_error_at: int | None = None

    async def validate_retention(self) -> None:
        self.retention_validations += 1
        if self.retention_error is not None:
            raise self.retention_error

    async def bootstrap(self, verifier: object, *, mode: SchemaMode) -> None:
        del verifier
        self.bootstrap_modes.append(mode)
        self.layout_validations += 1
        if self.layout_validations == self.layout_error_at:
            operation = "schema_validation"
            reason = "physical_drift"
            raise ClickHouseSchemaDriftError(operation, reason) from None

    async def read_history(self, plan: SchemaPlan) -> MigrationHistory:
        del plan
        self.history_reads += 1
        if self.history_reads == self.history_error_at and self.history_error is not None:
            raise self.history_error
        if self.history_versions:
            self.applied_version = self.history_versions.pop(0)
        return MigrationHistory(self.applied_version, ())

    async def record_migration(self, migration: MigrationDefinition) -> None:
        self.recorded.append(migration)
        if self.advance_on_record:
            self.applied_version = migration.version
        if self.record_event is not None:
            self.record_event(self, migration)

    async def ensure_namespace(self, *, mode: SchemaMode) -> None:
        self.namespace_modes.append(mode)


@dataclass(slots=True)
class _CapturingRunner:
    calls: list[tuple[SchemaMode, SchemaActor]] = field(default_factory=list)

    async def run(self, *, mode: SchemaMode, actor: SchemaActor) -> None:
        self.calls.append((mode, actor))


@pytest.mark.asyncio
async def test_validate_runs_two_complete_read_only_barriers() -> None:
    """Validate current history, physical target and namespace twice."""
    plan = synthetic_plan()
    inspector = _StateInspector(plan.migrations[-1].target)
    registry = _StateRegistry(applied_version=1)
    gateway = ScriptedGateway()

    await _runner(gateway, inspector, registry, plan).run(mode="validate", actor=SchemaActor.WORKER)

    assert registry.bootstrap_modes == ["validate", "validate", "validate"]
    assert registry.retention_validations == 0
    assert registry.layout_validations == _METADATA_VALIDATIONS_PER_RUN
    expected_history_reads = 2
    assert registry.history_reads == expected_history_reads
    assert registry.recorded == []
    assert registry.namespace_modes == ["validate", "validate"]
    assert inspector.validated == [plan.migrations[-1].target, plan.migrations[-1].target]
    assert gateway.commands == []


@pytest.mark.asyncio
async def test_migrate_rejects_unrepresentable_retention_before_schema_writes() -> None:
    """Run the server-clock preflight before metadata bootstrap or migration DDL."""
    plan = synthetic_plan()
    inspector = _StateInspector(plan.migrations[0].steps[0].before)
    retention_error = ClickHouseNamespaceError(
        "namespace_validate",
        "retention_unrepresentable",
    )
    registry = _StateRegistry(applied_version=0, retention_error=retention_error)
    gateway = ScriptedGateway()

    with pytest.raises(ClickHouseNamespaceError, match="retention_unrepresentable"):
        await _runner(gateway, inspector, registry, plan).run(
            mode="migrate",
            actor=SchemaActor.WORKER,
        )

    assert registry.retention_validations == 1
    assert registry.bootstrap_modes == []
    assert registry.history_reads == 0
    assert registry.namespace_modes == []
    assert gateway.commands == []


@pytest.mark.asyncio
async def test_validate_missing_history_fails_before_namespace_or_target() -> None:
    """Fail missing migration evidence without writes or false readiness."""
    plan = synthetic_plan()
    inspector = _StateInspector(plan.migrations[-1].target)
    registry = _StateRegistry(applied_version=0)

    with pytest.raises(ClickHouseMigrationError, match="migration_missing"):
        await _runner(ScriptedGateway(), inspector, registry, plan).run(
            mode="validate",
            actor=SchemaActor.WORKER,
        )

    assert registry.namespace_modes == []
    assert inspector.validated == []


@pytest.mark.asyncio
async def test_empty_plan_still_crosses_metadata_and_namespace_barrier() -> None:
    """Support TASK-015 before TASK-005 registers migration v1."""
    inspector = _StateInspector(SchemaContract())
    registry = _StateRegistry(applied_version=0)

    await _runner(ScriptedGateway(), inspector, registry, SchemaPlan(())).run(
        mode="migrate",
        actor=SchemaActor.WORKER,
    )

    assert inspector.validated == []
    assert registry.namespace_modes == ["migrate", "validate"]


@pytest.mark.asyncio
async def test_auto_migration_executes_once_then_records_and_rereads() -> None:
    """Apply DDL from exact before state and append history only afterward."""
    plan = synthetic_plan()
    migration = plan.migrations[0]
    inspector = _StateInspector(migration.steps[0].before)
    registry = _StateRegistry(applied_version=0)

    def apply_after(_gateway: ScriptedGateway) -> None:
        inspector.current = migration.steps[0].after

    gateway = ScriptedGateway(command_events=[apply_after])
    await _runner(gateway, inspector, registry, plan).run(mode="migrate", actor=SchemaActor.WORKER)

    assert gateway.commands == [
        (migration.steps[0].ddl, migration.steps[0].query_parameters),
    ]
    assert registry.recorded == [migration]
    assert registry.applied_version == 1
    expected_history_reads = 4
    assert registry.history_reads == expected_history_reads
    assert registry.namespace_modes == ["migrate", "validate"]


@pytest.mark.asyncio
async def test_migration_recovers_ddl_before_history_without_reexecuting() -> None:
    """Recognize a satisfied after postcondition and record crash recovery."""
    plan = synthetic_plan()
    migration = plan.migrations[0]
    inspector = _StateInspector(migration.target)
    registry = _StateRegistry(applied_version=0)
    gateway = ScriptedGateway()

    await _runner(gateway, inspector, registry, plan).run(mode="migrate", actor=SchemaActor.WORKER)

    assert gateway.commands == []
    assert registry.recorded == [migration]


@pytest.mark.asyncio
async def test_migration_rechecks_after_when_concurrent_ddl_wins_race() -> None:
    """Converge when another runner applies DDL between the two probes."""
    plan = synthetic_plan()
    migration = plan.migrations[0]
    inspector = _StateInspector(
        migration.target,
        match_results=[False, False, True],
    )
    registry = _StateRegistry(applied_version=0)

    await _runner(ScriptedGateway(), inspector, registry, plan).run(
        mode="migrate",
        actor=SchemaActor.WORKER,
    )

    assert registry.recorded == [migration]


@pytest.mark.asyncio
async def test_migration_accepts_concurrent_history_advancing_past_version() -> None:
    """Do not validate an obsolete target after another runner reaches v2."""
    plan = _two_migration_plan()
    first = plan.migrations[0]
    final_target = plan.migrations[-1].target
    inspector = _StateInspector(first.steps[0].before)

    def apply_first(_gateway: ScriptedGateway) -> None:
        inspector.current = first.target

    def advance_to_final(
        state: _StateRegistry,
        _migration: MigrationDefinition,
    ) -> None:
        state.applied_version = plan.target_version
        inspector.current = final_target

    registry = _StateRegistry(applied_version=0, record_event=advance_to_final)
    await _runner(
        ScriptedGateway(command_events=[apply_first]),
        inspector,
        registry,
        plan,
    ).run(mode="migrate", actor=SchemaActor.WORKER)

    assert registry.recorded == [first]
    assert inspector.validated[-1] == final_target


@pytest.mark.asyncio
async def test_migration_accepts_drift_after_concurrent_history_commit() -> None:
    """Resolve an obsolete physical view through newer durable history."""
    plan = _two_migration_plan()
    final_target = plan.migrations[-1].target
    registry = _StateRegistry(applied_version=0)

    def advance_history() -> None:
        registry.applied_version = plan.target_version

    inspector = _StateInspector(
        final_target,
        match_event=advance_history,
    )

    await _runner(ScriptedGateway(), inspector, registry, plan).run(
        mode="migrate",
        actor=SchemaActor.WORKER,
    )

    assert registry.recorded == []
    assert inspector.validated[-1] == final_target


@pytest.mark.asyncio
async def test_migration_skips_versions_observed_before_next_apply() -> None:
    """Skip definitions completed after the initial history snapshot."""
    plan = _two_migration_plan()
    final_target = plan.migrations[-1].target
    registry = _StateRegistry(
        applied_version=0,
        history_versions=[0, plan.target_version],
    )

    await _runner(
        ScriptedGateway(),
        _StateInspector(final_target),
        registry,
        plan,
    ).run(mode="migrate", actor=SchemaActor.WORKER)

    assert registry.recorded == []


@pytest.mark.asyncio
async def test_migration_rejects_regressing_history_snapshot() -> None:
    """Fail closed if an endpoint returns less history than already observed."""
    plan = _two_migration_plan()
    registry = _StateRegistry(applied_version=0, history_versions=[1, 0])

    with pytest.raises(ClickHouseMigrationError, match="history_not_advanced"):
        await _runner(
            ScriptedGateway(),
            _StateInspector(plan.migrations[0].target),
            registry,
            plan,
        ).run(mode="migrate", actor=SchemaActor.WORKER)

    assert registry.recorded == []


@pytest.mark.asyncio
async def test_concurrent_recovery_read_failure_has_no_drift_context() -> None:
    """Keep a failed recovery read outside the caught physical-drift context."""
    plan = synthetic_plan()
    recovery_error = ClickHouseBackendIOError("migration_history_read", "database_error")
    registry = _StateRegistry(
        applied_version=0,
        history_error_at=3,
        history_error=recovery_error,
    )

    with pytest.raises(ClickHouseBackendIOError) as raised:
        await _runner(
            ScriptedGateway(),
            _StateInspector(SchemaContract()),
            registry,
            plan,
        ).run(mode="migrate", actor=SchemaActor.WORKER)

    assert raised.value is recovery_error
    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None


@pytest.mark.asyncio
async def test_unrecovered_physical_drift_preserves_safe_report() -> None:
    """Rebuild value-free drift after the required recovery history read."""
    plan = synthetic_plan()
    table = plan.migrations[-1].target.tables[0].table
    report = SchemaDriftReport(
        mismatch_count=1,
        locations=(
            SchemaDriftLocation(
                table=table,
                path="columns.result_payload.type",
            ),
        ),
    )
    inspector = _StateInspector(
        plan.migrations[-1].target,
        validation_error=_PhysicalSchemaDriftError(report),
    )

    with pytest.raises(_PhysicalSchemaDriftError) as raised:
        await _runner(
            ScriptedGateway(),
            inspector,
            _StateRegistry(applied_version=0),
            plan,
        ).run(mode="migrate", actor=SchemaActor.WORKER)

    assert raised.value.report is report
    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None


@pytest.mark.asyncio
async def test_final_barrier_revalidates_metadata_layout() -> None:
    """Fail if permanent metadata drifts after the first complete read."""
    plan = synthetic_plan()
    inspector = _StateInspector(plan.migrations[-1].target)
    registry = _StateRegistry(
        applied_version=plan.target_version,
        layout_error_at=_METADATA_VALIDATIONS_PER_RUN,
    )

    with pytest.raises(ClickHouseSchemaDriftError, match="physical_drift"):
        await _runner(ScriptedGateway(), inspector, registry, plan).run(
            mode="migrate",
            actor=SchemaActor.WORKER,
        )

    assert registry.layout_validations == _METADATA_VALIDATIONS_PER_RUN
    assert registry.namespace_modes == ["migrate"]


@pytest.mark.asyncio
async def test_migration_rejects_state_matching_neither_before_nor_after() -> None:
    """Never execute DDL over an unknown partial physical state."""
    plan = synthetic_plan()
    registry = _StateRegistry(applied_version=0)

    with pytest.raises(ClickHouseSchemaDriftError, match="partial_state"):
        await _runner(
            ScriptedGateway(),
            _StateInspector(SchemaContract()),
            registry,
            plan,
        ).run(mode="migrate", actor=SchemaActor.WORKER)

    assert registry.recorded == []


@pytest.mark.asyncio
async def test_worker_refuses_controlled_migration_before_ddl() -> None:
    """Reserve CONTROLLED definitions for the operator manager path."""
    plan = synthetic_plan(execution=MigrationExecution.CONTROLLED)
    inspector = _StateInspector(plan.migrations[0].steps[0].before)
    gateway = ScriptedGateway()

    with pytest.raises(ClickHouseMigrationError, match="controlled_pending"):
        await _runner(gateway, inspector, _StateRegistry(0), plan).run(
            mode="migrate",
            actor=SchemaActor.WORKER,
        )

    assert gateway.commands == []


@pytest.mark.asyncio
async def test_manager_applies_controlled_migration() -> None:
    """Allow the explicit manager actor to cross the controlled boundary."""
    plan = synthetic_plan(execution=MigrationExecution.CONTROLLED)
    migration = plan.migrations[0]
    inspector = _StateInspector(migration.steps[0].before)

    def apply_after(_gateway: ScriptedGateway) -> None:
        inspector.current = migration.target

    gateway = ScriptedGateway(command_events=[apply_after])
    registry = _StateRegistry(0)

    await _runner(gateway, inspector, registry, plan).run(mode="migrate", actor=SchemaActor.MANAGER)

    assert registry.recorded == [migration]


@pytest.mark.asyncio
async def test_ambiguous_ddl_uses_after_postcondition_without_retry() -> None:
    """Accept response loss only when the exact after state is observable."""
    plan = synthetic_plan()
    migration = plan.migrations[0]
    inspector = _StateInspector(migration.steps[0].before)

    def commit_then_lose(_gateway: ScriptedGateway) -> None:
        inspector.current = migration.target
        raise AmbiguousClickHouseError

    gateway = ScriptedGateway(command_events=[commit_then_lose])
    registry = _StateRegistry(0)

    await _runner(gateway, inspector, registry, plan).run(mode="migrate", actor=SchemaActor.WORKER)

    assert len(gateway.commands) == 1
    assert registry.recorded == [migration]


@pytest.mark.asyncio
async def test_ambiguous_or_definite_ddl_failure_is_fail_closed() -> None:
    """Reject unconfirmed response loss and definite server errors."""
    plan = synthetic_plan()
    before = plan.migrations[0].steps[0].before
    ambiguous_gateway = ScriptedGateway(command_events=[AmbiguousClickHouseError()])
    with pytest.raises(ClickHouseBackendIOError, match="ddl_unconfirmed"):
        await _runner(ambiguous_gateway, _StateInspector(before), _StateRegistry(0), plan).run(
            mode="migrate",
            actor=SchemaActor.WORKER,
        )

    definite_gateway = ScriptedGateway(command_events=[DefiniteClickHouseError()])
    with pytest.raises(ClickHouseMigrationError, match="database_error"):
        await _runner(definite_gateway, _StateInspector(before), _StateRegistry(0), plan).run(
            mode="migrate",
            actor=SchemaActor.WORKER,
        )


@pytest.mark.asyncio
async def test_barrier_rejects_history_that_did_not_advance() -> None:
    """Require an acknowledged history reread after every physical migration."""
    plan = synthetic_plan()
    migration = plan.migrations[0]
    inspector = _StateInspector(migration.target)
    registry = _StateRegistry(0, advance_on_record=False)

    with pytest.raises(ClickHouseMigrationError, match="history_not_advanced"):
        await _runner(ScriptedGateway(), inspector, registry, plan).run(
            mode="migrate",
            actor=SchemaActor.WORKER,
        )


@pytest.mark.asyncio
async def test_public_barrier_sanitizes_invalid_inspection_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Remove raw inspection messages and traceback contexts."""
    raw_error = SchemaInspectionError("secret-endpoint")

    async def fail_barrier(*_arguments: object, **_options: object) -> None:
        raise raw_error

    monkeypatch.setattr(runner_module, "_run_context_barrier", fail_barrier)
    with pytest.raises(ClickHouseBackendIOError, match="invalid_response") as raised:
        await runner_module.run_schema_barrier(
            _context(),
            cast("AsyncClient", object()),
            mode="validate",
            actor=SchemaActor.WORKER,
        )

    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None
    assert "secret-endpoint" not in str(raised.value)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "raw_error",
    [
        pytest.param(
            ClickHouseMigrationError("migration_barrier", "migration_missing"),
            id="package-error",
        ),
        pytest.param(asyncio.CancelledError(), id="cancellation"),
    ],
)
async def test_public_barrier_preserves_package_error_and_cancellation_identity(
    monkeypatch: pytest.MonkeyPatch,
    raw_error: BaseException,
) -> None:
    """Do not double-wrap safe package errors or caller cancellation."""

    async def fail_barrier(*_arguments: object, **_options: object) -> None:
        raise raw_error

    monkeypatch.setattr(runner_module, "_run_context_barrier", fail_barrier)
    with pytest.raises(type(raw_error)) as raised:
        await runner_module.run_schema_barrier(
            _context(),
            cast("AsyncClient", object()),
            mode="validate",
            actor=SchemaActor.WORKER,
        )
    assert raised.value is raw_error


@pytest.mark.asyncio
async def test_public_barrier_returns_after_successful_internal_run(monkeypatch: pytest.MonkeyPatch) -> None:
    """Exercise the success branch without manufacturing a failure reason."""

    async def pass_barrier(*_arguments: object, **_options: object) -> None:
        return None

    monkeypatch.setattr(runner_module, "_run_context_barrier", pass_barrier)

    await runner_module.run_schema_barrier(
        _context(),
        cast("AsyncClient", object()),
        mode="validate",
        actor=SchemaActor.WORKER,
    )


@pytest.mark.asyncio
async def test_context_barrier_passes_explicit_actor_to_runner(monkeypatch: pytest.MonkeyPatch) -> None:
    """Use the immutable schema context without reaching into backend state."""
    context = _context()
    capturing = _CapturingRunner()

    def build_runner(
        received_context: SchemaBarrierContext,
        client: object,
    ) -> _CapturingRunner:
        assert received_context is context
        assert client is fake_client
        return capturing

    fake_client = cast("AsyncClient", object())
    monkeypatch.setattr(runner_module, "_build_runner", build_runner)

    await runner_module._run_context_barrier(  # noqa: SLF001
        context,
        fake_client,
        mode="migrate",
        actor=SchemaActor.MANAGER,
    )

    assert capturing.calls == [("migrate", SchemaActor.MANAGER)]


def test_runner_factory_uses_explicit_schema_context() -> None:
    """Build concrete collaborators only from the schema-owned context."""
    context = _context(plan=SchemaPlan(()))
    built = runner_module._build_runner(  # noqa: SLF001
        context,
        cast("AsyncClient", object()),
    )
    assert built.plan == SchemaPlan(())
    assert isinstance(built.registry, MetadataRegistry)
    assert built.registry.namespace_contract == namespace_contract()
    assert built.registry.package_version == context.package_version


@pytest.mark.parametrize("package_version", ["", cast("str", object())])
def test_schema_context_rejects_missing_package_identity(package_version: str) -> None:
    """Reject an unusable distribution identity before creating a gateway."""
    with pytest.raises(ValueError, match="package_version must be a non-empty string"):
        SchemaBarrierContext(
            namespace_contract=namespace_contract(),
            plan=synthetic_plan(),
            package_version=package_version,
        )


def _runner(
    gateway: ScriptedGateway,
    inspector: _StateInspector,
    registry: _StateRegistry,
    plan: SchemaPlan,
) -> SchemaRunner:
    return SchemaRunner(
        gateway=gateway,
        inspector=inspector,
        registry=registry,
        plan=plan,
    )


def _context(*, plan: SchemaPlan | None = None) -> SchemaBarrierContext:
    return SchemaBarrierContext(
        namespace_contract=namespace_contract(),
        plan=synthetic_plan() if plan is None else plan,
        package_version="0.1.0",
    )


def _two_migration_plan() -> SchemaPlan:
    first = synthetic_plan().migrations[0]
    audit_table = QualifiedTable(Identifier("test_db"), Identifier("synthetic_audit"))
    audit_contract = TableContract(
        table=audit_table,
        columns=(ColumnContract(Identifier("sequence"), "UInt64"),),
        engine="MergeTree",
        partition_key="",
        sorting_key="sequence",
        primary_key="sequence",
    )
    final_target = SchemaContract(tables=(*first.target.tables, audit_contract))
    second = MigrationDefinition(
        version=2,
        name="create_synthetic_audit",
        execution=MigrationExecution.AUTO,
        reentrant=True,
        concurrent_safe=True,
        steps=(
            MigrationStep(
                ddl=(
                    "CREATE TABLE IF NOT EXISTS `test_db`.`synthetic_audit` "
                    "(`sequence` UInt64) ENGINE=MergeTree ORDER BY sequence"
                ),
                before=first.target,
                after=final_target,
            ),
        ),
    )
    return SchemaPlan((first, second))
