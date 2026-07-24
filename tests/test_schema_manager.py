"""Verify the public schema-manager facade against one runtime collaborator."""

import asyncio
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING, Any, cast

import pytest

from taskiq_clickhouse._types import SchemaActor, SchemaMode
from taskiq_clickhouse.backend import ClickHouseResultBackend
from taskiq_clickhouse.exceptions import ClickHouseLifecycleError
from taskiq_clickhouse.schema import ClickHouseSchemaManager


if TYPE_CHECKING:
    from taskiq_clickhouse._backend_runtime import BackendRuntime


_ASYNC_TIMEOUT = 2.0


@dataclass(slots=True)
class _RuntimeSpy:
    """Record schema-manager delegation without emulating lifecycle internals."""

    release: asyncio.Event | None = None
    error: BaseException | None = None
    calls: list[tuple[SchemaMode, SchemaActor]] = field(default_factory=list)
    entered: asyncio.Event = field(default_factory=asyncio.Event)

    async def run_schema_manager(
        self,
        *,
        mode: SchemaMode,
        actor: SchemaActor,
    ) -> None:
        """Record one call and apply only the requested synchronization behavior."""
        self.calls.append((mode, actor))
        self.entered.set()
        if self.release is not None:
            await self.release.wait()
        if self.error is not None:
            raise self.error


def _backend_with_runtime(runtime: _RuntimeSpy) -> ClickHouseResultBackend[object]:
    backend = ClickHouseResultBackend[object](
        host="localhost",
        database="tasks",
        secure=False,
        result_ttl=timedelta(days=1),
        purge_ttl=timedelta(days=7),
    )
    return _install_runtime(backend, runtime)


def _install_runtime(
    backend: ClickHouseResultBackend[object],
    runtime: _RuntimeSpy,
) -> ClickHouseResultBackend[object]:
    backend._runtime = cast("BackendRuntime", runtime)  # noqa: SLF001 - one composition seam.
    return backend


async def _wait(event: asyncio.Event) -> None:
    async with asyncio.timeout(_ASYNC_TIMEOUT):
        await event.wait()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method_name", "expected_call"),
    [
        ("migrate", ("migrate", SchemaActor.MANAGER)),
        ("validate", ("validate", SchemaActor.WORKER)),
    ],
)
async def test_manager_delegates_exact_barrier_policy_once(
    method_name: str,
    expected_call: tuple[SchemaMode, SchemaActor],
) -> None:
    """Map each public operation to its exact runtime mode and actor."""
    runtime = _RuntimeSpy()
    manager = ClickHouseSchemaManager(_backend_with_runtime(runtime))

    if method_name == "migrate":
        await manager.migrate()
    else:
        await manager.validate()

    assert runtime.calls == [expected_call]


@pytest.mark.asyncio
async def test_manager_accepts_backend_subclasses_and_rejects_other_objects() -> None:
    """Retain the declared backend boundary without demanding an exact class."""

    class BackendSubclass(ClickHouseResultBackend[object]):
        pass

    with pytest.raises(TypeError, match="ClickHouseResultBackend"):
        ClickHouseSchemaManager(cast("Any", object()))

    backend = BackendSubclass(
        host="localhost",
        database="tasks",
        secure=False,
        result_ttl=timedelta(days=1),
        purge_ttl=timedelta(days=7),
    )
    runtime = _RuntimeSpy()
    _install_runtime(backend, runtime)

    await ClickHouseSchemaManager(backend).validate()

    assert runtime.calls == [("validate", SchemaActor.WORKER)]


@pytest.mark.asyncio
async def test_manager_claims_single_use_before_first_await() -> None:
    """Reject concurrent and later calls after the synchronous claim."""
    release = asyncio.Event()
    runtime = _RuntimeSpy(release=release)
    manager = ClickHouseSchemaManager(_backend_with_runtime(runtime))
    first_call = asyncio.create_task(manager.migrate())
    await _wait(runtime.entered)

    with pytest.raises(ClickHouseLifecycleError, match="already_used"):
        await manager.validate()

    release.set()
    await first_call

    with pytest.raises(ClickHouseLifecycleError, match="already_used"):
        await manager.migrate()
    assert runtime.calls == [("migrate", SchemaActor.MANAGER)]


@pytest.mark.asyncio
async def test_manager_propagates_runtime_failure_and_remains_used() -> None:
    """Leave lifecycle classification to runtime while preserving single use."""
    runtime_error = ClickHouseLifecycleError("schema_manager", "backend_not_new")
    runtime = _RuntimeSpy(error=runtime_error)
    manager = ClickHouseSchemaManager(_backend_with_runtime(runtime))

    with pytest.raises(ClickHouseLifecycleError) as raised:
        await manager.validate()

    assert raised.value is not runtime_error
    assert raised.value.operation == runtime_error.operation
    assert raised.value.reason == runtime_error.reason
    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None
    with pytest.raises(ClickHouseLifecycleError, match="already_used"):
        await manager.validate()
    assert runtime.calls == [("validate", SchemaActor.WORKER)]
