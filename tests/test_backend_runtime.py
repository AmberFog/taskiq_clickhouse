"""Verify the backend runtime state machine through injected collaborators."""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from functools import partial
import os
from threading import Event
import traceback
from typing import TYPE_CHECKING, cast

import pytest

from taskiq_clickhouse._backend_runtime import (
    BackendRuntime,
    LifecycleState,
    RuntimeDependencies,
)
from taskiq_clickhouse._schema_drift import (
    SchemaDriftLocation,
    SchemaDriftReport,
)
from taskiq_clickhouse._storage.layout import storage_layout_from_names
from taskiq_clickhouse._storage.repository import StorageRepository
from taskiq_clickhouse._storage_policy import NamespaceKey, RetentionPolicy, StoragePolicy
from taskiq_clickhouse._types import SchemaActor, SchemaMode
from taskiq_clickhouse.exceptions import (
    ClickHouseBackendIOError,
    ClickHouseLifecycleError,
    ClickHouseNamespaceError,
    ClickHouseResultBackendError,
    ClickHouseSchemaError,
    _PhysicalSchemaDriftError,
)


if TYPE_CHECKING:
    from collections.abc import Mapping

    from clickhouse_connect.driver.asyncclient import AsyncClient

    from taskiq_clickhouse._backend_runtime import RepositoryFactory
    from taskiq_clickhouse._clickhouse.request import InsertRequest
    from taskiq_clickhouse._client_lifecycle import ClientFactory
    from taskiq_clickhouse._lifecycle import SchemaBarrier


_ASYNC_TIMEOUT = 2.0
_THREAD_TIMEOUT = 2.0
_NAMESPACE_OPERATION = "namespace_validate"
_NAMESPACE_CONFLICT = "namespace_conflict"
_RESULT_TTL_US = 1
_PURGE_TTL_US = 2


class _FakeClient:
    """Small observable substitute for the runtime-owned ClickHouse client."""

    def __init__(
        self,
        *,
        close_release: asyncio.Event | None = None,
        close_error: BaseException | None = None,
    ) -> None:
        self.close_calls = 0
        self.close_started = asyncio.Event()
        self.close_release = close_release
        self.close_error = close_error

    async def close(self) -> None:
        """Record and optionally suspend or fail the single close operation."""
        self.close_calls += 1
        self.close_started.set()
        if self.close_release is not None:
            await self.close_release.wait()
        if self.close_error is not None:
            raise self.close_error


class _FatalRuntimeError(BaseException):
    """Synthetic fatal failure whose object identity is part of the contract."""


class _UnusedGateway:
    """Fail if a runtime-only repository unexpectedly performs storage I/O."""

    async def query_rows(
        self,
        query: str,
        *,
        query_parameters: Mapping[str, object] | None = None,
        settings: Mapping[str, object] | None = None,
        column_formats: Mapping[str, str] | None = None,
    ) -> tuple[tuple[object, ...], ...]:
        del query, query_parameters, settings, column_formats
        message = "runtime test unexpectedly queried storage"
        raise AssertionError(message)

    async def insert_rows(self, request: InsertRequest) -> None:
        del request
        message = "runtime test unexpectedly inserted storage data"
        raise AssertionError(message)


@dataclass(slots=True)
class _SchemaBarrierProbe:
    """Provide one exact barrier double with observable synchronization."""

    release: asyncio.Event | None = None
    error: BaseException | None = None
    calls: list[tuple[object, SchemaMode, SchemaActor]] = field(default_factory=list)
    entered: asyncio.Event = field(default_factory=asyncio.Event)

    async def __call__(
        self,
        client: AsyncClient,
        *,
        mode: SchemaMode,
        actor: SchemaActor,
    ) -> None:
        """Record, block and fail only as explicitly configured by a test."""
        self.calls.append((client, mode, actor))
        self.entered.set()
        if self.release is not None:
            await self.release.wait()
        if self.error is not None:
            raise self.error


@dataclass(slots=True)
class _ClientFactoryProbe:
    """Provide one exact client factory with observable synchronization."""

    client: _FakeClient
    release: asyncio.Event | None = None
    error: BaseException | None = None
    calls: int = 0
    entered: asyncio.Event = field(default_factory=asyncio.Event)

    async def __call__(self) -> AsyncClient:
        """Record, block and fail before returning the configured client."""
        self.calls += 1
        self.entered.set()
        if self.release is not None:
            await self.release.wait()
        if self.error is not None:
            raise self.error
        return await _return_client(self.client)


async def _return_client(client: _FakeClient) -> AsyncClient:
    return cast("AsyncClient", client)


async def _pass_barrier(
    client: AsyncClient,
    *,
    mode: SchemaMode,
    actor: SchemaActor,
) -> None:
    del client, mode, actor


def _constant_repository(
    repository: StorageRepository,
    client: AsyncClient,
) -> StorageRepository:
    del client
    return repository


def _make_runtime(
    client_factory: ClientFactory,
    *,
    schema_barrier: SchemaBarrier = _pass_barrier,
    repository_factory: RepositoryFactory | None = None,
    schema_mode: SchemaMode = "migrate",
) -> tuple[BackendRuntime, StorageRepository]:
    repository = StorageRepository(
        gateway=_UnusedGateway(),
        layout=storage_layout_from_names("tasks", "results", "progress"),
        policy=StoragePolicy(
            NamespaceKey("runtime-tests"),
            RetentionPolicy(_RESULT_TTL_US, _PURGE_TTL_US),
        ),
    )
    selected_repository_factory = repository_factory or partial(
        _constant_repository,
        repository,
    )
    dependencies = RuntimeDependencies(
        client_factory=client_factory,
        schema_runner=schema_barrier,
        repository_factory=selected_repository_factory,
    )
    return BackendRuntime(dependencies, schema_mode=schema_mode), repository


async def _wait(event: asyncio.Event) -> None:
    async with asyncio.timeout(_ASYNC_TIMEOUT):
        await event.wait()


def _assert_detached(error: ClickHouseResultBackendError) -> None:
    assert error.__cause__ is None
    assert error.__context__ is None


def _assert_state(runtime: BackendRuntime, expected: LifecycleState) -> None:
    assert runtime.state is expected


@pytest.mark.asyncio
async def test_startup_owns_one_ready_repository_and_shutdown_is_terminal() -> None:
    """Create one client, cross one worker barrier and close it exactly once."""
    client = _FakeClient()
    client_factory = _ClientFactoryProbe(client)
    barrier_calls: list[tuple[object, SchemaMode, SchemaActor, LifecycleState]] = []
    repository_clients: list[object] = []
    runtime: BackendRuntime

    async def barrier(
        client: AsyncClient,
        *,
        mode: SchemaMode,
        actor: SchemaActor,
    ) -> None:
        barrier_calls.append((client, mode, actor, runtime.state))

    def repository_factory(client: AsyncClient) -> StorageRepository:
        repository_clients.append(client)
        return repository

    runtime, repository = _make_runtime(
        client_factory,
        schema_barrier=barrier,
        repository_factory=repository_factory,
    )

    await runtime.startup()
    await runtime.startup()

    _assert_state(runtime, LifecycleState.READY)
    assert not runtime.is_new
    assert runtime.repository() is repository
    assert client_factory.calls == 1
    assert barrier_calls == [(client, "migrate", SchemaActor.WORKER, LifecycleState.STARTING)]
    assert repository_clients == [client]

    await runtime.shutdown()
    await runtime.shutdown()

    _assert_state(runtime, LifecycleState.CLOSED)
    assert client.close_calls == 1
    with pytest.raises(ClickHouseLifecycleError, match="closed"):
        await runtime.startup()
    with pytest.raises(ClickHouseLifecycleError, match="not_ready"):
        runtime.repository()


@pytest.mark.asyncio
async def test_shutdown_of_new_runtime_never_creates_client() -> None:
    """Make a never-started runtime terminal without acquiring resources."""
    client_factory = _ClientFactoryProbe(_FakeClient())

    runtime, _repository = _make_runtime(client_factory)

    await runtime.shutdown()
    await runtime.shutdown()

    assert runtime.state is LifecycleState.CLOSED
    assert client_factory.calls == 0


@pytest.mark.asyncio
async def test_concurrent_startups_share_one_successful_transition() -> None:
    """Serialize READY callers behind the same factory and schema barrier."""
    client = _FakeClient()
    barrier_release = asyncio.Event()
    barrier = _SchemaBarrierProbe(release=barrier_release)
    second_called = asyncio.Event()
    client_factory = _ClientFactoryProbe(client)

    async def second_startup(runtime: BackendRuntime) -> None:
        second_called.set()
        await runtime.startup()

    runtime, _repository = _make_runtime(client_factory, schema_barrier=barrier)
    first = asyncio.create_task(runtime.startup())
    await _wait(barrier.entered)
    second = asyncio.create_task(second_startup(runtime))
    await _wait(second_called)

    assert not second.done()
    barrier_release.set()
    await asyncio.gather(first, second)

    assert client_factory.calls == 1
    assert runtime.state is LifecycleState.READY
    await runtime.shutdown()


@pytest.mark.asyncio
async def test_concurrent_startups_replay_safe_failure_then_allow_retry() -> None:
    """Replay one failed attempt to its waiter without consuming the retry client."""
    failed_client = _FakeClient()
    retry_client = _FakeClient()
    clients = iter((failed_client, retry_client))
    barrier_release = asyncio.Event()
    barrier = _SchemaBarrierProbe(
        release=barrier_release,
        error=ClickHouseNamespaceError(_NAMESPACE_OPERATION, _NAMESPACE_CONFLICT),
    )
    second_called = asyncio.Event()

    async def client_factory() -> AsyncClient:
        return await _return_client(next(clients))

    async def second_startup(runtime: BackendRuntime) -> None:
        second_called.set()
        await runtime.startup()

    runtime, _repository = _make_runtime(client_factory, schema_barrier=barrier)
    first = asyncio.create_task(runtime.startup())
    await _wait(barrier.entered)
    second = asyncio.create_task(second_startup(runtime))
    await _wait(second_called)
    barrier_release.set()
    outcomes = await asyncio.gather(first, second, return_exceptions=True)

    assert tuple(type(outcome) for outcome in outcomes) == (
        ClickHouseNamespaceError,
        ClickHouseNamespaceError,
    )
    for outcome in outcomes:
        assert isinstance(outcome, ClickHouseResultBackendError)
        _assert_detached(outcome)
    assert failed_client.close_calls == 1
    _assert_state(runtime, LifecycleState.NEW)

    barrier.error = None
    await runtime.startup()
    _assert_state(runtime, LifecycleState.READY)
    await runtime.shutdown()
    assert retry_client.close_calls == 1


@pytest.mark.asyncio
async def test_concurrent_startups_replay_detached_physical_drift() -> None:
    """Replay structured drift without exposing values or invoking the wrong constructor."""
    secret = "password=physical-secret dsn=https://private.internal"  # noqa: S105  # pragma: allowlist secret
    client = _FakeClient()
    barrier_release = asyncio.Event()
    table = storage_layout_from_names("tasks", "results", "progress").result_table
    report = SchemaDriftReport(
        mismatch_count=1,
        locations=(SchemaDriftLocation(table, "engine"),),
    )
    internal_error = _PhysicalSchemaDriftError(report)
    internal_error.__context__ = RuntimeError(secret)
    barrier = _SchemaBarrierProbe(release=barrier_release, error=internal_error)
    second_called = asyncio.Event()

    async def second_startup(runtime: BackendRuntime) -> None:
        second_called.set()
        await runtime.startup()

    runtime, _repository = _make_runtime(
        partial(_return_client, client),
        schema_barrier=barrier,
    )
    first = asyncio.create_task(runtime.startup())
    await _wait(barrier.entered)
    second = asyncio.create_task(second_startup(runtime))
    await _wait(second_called)
    barrier_release.set()
    outcomes = await asyncio.gather(first, second, return_exceptions=True)

    assert all(type(outcome) is _PhysicalSchemaDriftError for outcome in outcomes)
    assert outcomes[0] is not outcomes[1]
    for outcome in outcomes:
        assert isinstance(outcome, _PhysicalSchemaDriftError)
        assert outcome.report is report
        _assert_detached(outcome)
        rendered = repr(outcome) + repr(outcome.report) + "".join(traceback.format_exception(outcome))
        assert secret not in rendered
        assert "private.internal" not in rendered
    assert client.close_calls == 1
    _assert_state(runtime, LifecycleState.NEW)


@pytest.mark.asyncio
async def test_factory_failure_is_redacted_and_runtime_remains_retryable() -> None:
    """Discard raw client-creation details and restore the NEW state."""
    secret = "password=factory-secret private.internal"  # noqa: S105  # pragma: allowlist secret
    good_client = _FakeClient()
    client_factory = _ClientFactoryProbe(good_client, error=RuntimeError(secret))

    runtime, _repository = _make_runtime(client_factory)

    with pytest.raises(ClickHouseBackendIOError, match="client_create_failed") as raised:
        await runtime.startup()

    assert secret not in str(raised.value)
    _assert_detached(raised.value)
    _assert_state(runtime, LifecycleState.NEW)

    client_factory.error = None
    await runtime.startup()
    await runtime.shutdown()
    assert good_client.close_calls == 1


@pytest.mark.asyncio
async def test_repository_factory_failure_closes_client_and_restores_new() -> None:
    """Treat post-barrier composition failure as a failed startup transaction."""
    client = _FakeClient()
    secret = "password=repository-secret private.internal"  # noqa: S105  # pragma: allowlist secret

    def repository_factory(client: AsyncClient) -> StorageRepository:
        del client
        raise ValueError(secret)

    runtime, _repository = _make_runtime(
        partial(_return_client, client),
        repository_factory=repository_factory,
    )

    with pytest.raises(ClickHouseBackendIOError, match="runtime_init_failed") as raised:
        await runtime.startup()

    assert secret not in str(raised.value)
    _assert_detached(raised.value)
    assert client.close_calls == 1
    _assert_state(runtime, LifecycleState.NEW)


@pytest.mark.asyncio
async def test_repository_factory_fatal_failure_keeps_identity() -> None:
    """Close the client before re-raising a fatal composition failure."""
    client = _FakeClient(close_error=RuntimeError("unsafe close failure"))
    fatal = _FatalRuntimeError()

    def repository_factory(factory_client: AsyncClient) -> StorageRepository:
        del factory_client
        raise fatal

    runtime, _repository = _make_runtime(
        partial(_return_client, client),
        repository_factory=repository_factory,
    )

    with pytest.raises(_FatalRuntimeError) as raised:
        await runtime.startup()

    assert raised.value is fatal
    assert client.close_calls == 1
    _assert_state(runtime, LifecycleState.NEW)


@pytest.mark.asyncio
async def test_repository_factory_fatal_failure_survives_successful_cleanup() -> None:
    """Re-raise a fatal composition failure after an ordinary client close."""
    client = _FakeClient()
    fatal = _FatalRuntimeError()

    def repository_factory(factory_client: AsyncClient) -> StorageRepository:
        del factory_client
        raise fatal

    runtime, _repository = _make_runtime(
        partial(_return_client, client),
        repository_factory=repository_factory,
    )

    with pytest.raises(_FatalRuntimeError) as raised:
        await runtime.startup()

    assert raised.value is fatal
    assert client.close_calls == 1
    _assert_state(runtime, LifecycleState.NEW)


@pytest.mark.asyncio
async def test_invalid_client_factory_result_fails_before_repository() -> None:
    """Fail closed if a foreign dependency violates its typed client contract."""

    async def invalid_client_factory() -> AsyncClient:
        return cast("AsyncClient", None)

    runtime, _repository = _make_runtime(invalid_client_factory)

    with pytest.raises(ClickHouseBackendIOError, match="runtime_init_failed") as raised:
        await runtime.startup()

    _assert_detached(raised.value)
    _assert_state(runtime, LifecycleState.NEW)


@pytest.mark.asyncio
async def test_shutdown_waits_for_startup_and_closes_the_ready_client() -> None:
    """Prevent startup and shutdown from owning overlapping client phases."""
    client = _FakeClient()
    barrier_release = asyncio.Event()
    barrier = _SchemaBarrierProbe(release=barrier_release)
    shutdown_called = asyncio.Event()

    async def shutdown(runtime: BackendRuntime) -> None:
        shutdown_called.set()
        await runtime.shutdown()

    runtime, _repository = _make_runtime(
        partial(_return_client, client),
        schema_barrier=barrier,
    )
    startup = asyncio.create_task(runtime.startup())
    await _wait(barrier.entered)
    terminal = asyncio.create_task(shutdown(runtime))
    await _wait(shutdown_called)

    assert client.close_calls == 0
    barrier_release.set()
    await asyncio.gather(startup, terminal)

    _assert_state(runtime, LifecycleState.CLOSED)
    assert client.close_calls == 1


@pytest.mark.asyncio
async def test_schema_manager_and_worker_share_one_lifecycle_lease() -> None:
    """Close the manager client before startup creates its runtime client."""
    manager_client = _FakeClient()
    worker_client = _FakeClient()
    clients = iter((manager_client, worker_client))
    manager_started = asyncio.Event()
    manager_release = asyncio.Event()
    startup_called = asyncio.Event()
    observations: list[tuple[SchemaActor, int, int]] = []

    async def client_factory() -> AsyncClient:
        return await _return_client(next(clients))

    async def barrier(
        client: AsyncClient,
        *,
        mode: SchemaMode,
        actor: SchemaActor,
    ) -> None:
        del mode
        observations.append((actor, manager_client.close_calls, worker_client.close_calls))
        if client is cast("object", manager_client):
            manager_started.set()
            await manager_release.wait()

    async def startup(runtime: BackendRuntime) -> None:
        startup_called.set()
        await runtime.startup()

    runtime, _repository = _make_runtime(client_factory, schema_barrier=barrier)
    manager = asyncio.create_task(
        runtime.run_schema_manager(mode="migrate", actor=SchemaActor.MANAGER),
    )
    await _wait(manager_started)
    worker = asyncio.create_task(startup(runtime))
    await _wait(startup_called)

    assert observations == [(SchemaActor.MANAGER, 0, 0)]
    manager_release.set()
    await asyncio.gather(manager, worker)

    assert observations == [
        (SchemaActor.MANAGER, 0, 0),
        (SchemaActor.WORKER, 1, 0),
    ]
    assert runtime.state is LifecycleState.READY
    await runtime.shutdown()


@pytest.mark.asyncio
async def test_schema_manager_and_new_shutdown_do_not_overlap_clients() -> None:
    """Finish manager cleanup before a queued NEW shutdown becomes terminal."""
    client = _FakeClient()
    barrier_release = asyncio.Event()
    barrier = _SchemaBarrierProbe(release=barrier_release)
    shutdown_called = asyncio.Event()

    async def shutdown(runtime: BackendRuntime) -> None:
        shutdown_called.set()
        await runtime.shutdown()

    runtime, _repository = _make_runtime(
        partial(_return_client, client),
        schema_barrier=barrier,
    )
    manager = asyncio.create_task(
        runtime.run_schema_manager(mode="validate", actor=SchemaActor.WORKER),
    )
    await _wait(barrier.entered)
    terminal = asyncio.create_task(shutdown(runtime))
    await _wait(shutdown_called)

    assert runtime.state is LifecycleState.NEW
    assert client.close_calls == 0
    barrier_release.set()
    await asyncio.gather(manager, terminal)

    assert client.close_calls == 1
    _assert_state(runtime, LifecycleState.CLOSED)


@pytest.mark.asyncio
async def test_schema_manager_requires_new_runtime_without_creating_client() -> None:
    """Reject manager work after startup before allocating a temporary client."""
    worker_client = _FakeClient()
    unexpected_client = _FakeClient()
    clients = iter((worker_client, unexpected_client))
    factory_calls = 0

    async def client_factory() -> AsyncClient:
        nonlocal factory_calls
        factory_calls += 1
        return await _return_client(next(clients))

    runtime, _repository = _make_runtime(client_factory)
    await runtime.startup()

    with pytest.raises(ClickHouseLifecycleError, match="backend_not_new"):
        await runtime.run_schema_manager(mode="validate", actor=SchemaActor.WORKER)

    assert factory_calls == 1
    assert unexpected_client.close_calls == 0
    await runtime.shutdown()


@pytest.mark.asyncio
async def test_foreign_pid_cannot_use_or_close_ready_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reject inherited READY ownership before touching the client."""
    client = _FakeClient()
    runtime, _repository = _make_runtime(partial(_return_client, client))
    await runtime.startup()
    owner_pid = os.getpid()
    with monkeypatch.context() as scoped_patch:
        scoped_patch.setattr(os, "getpid", lambda: owner_pid + 1)

        with pytest.raises(ClickHouseLifecycleError, match="foreign_runtime"):
            runtime.repository()
        with pytest.raises(ClickHouseLifecycleError, match="foreign_runtime"):
            await runtime.startup()
        with pytest.raises(ClickHouseLifecycleError, match="foreign_runtime"):
            await runtime.shutdown()

    assert client.close_calls == 0
    await runtime.shutdown()


@pytest.mark.asyncio
async def test_foreign_event_loop_cannot_access_ready_runtime() -> None:
    """Reject same-process calls from another thread's event loop."""
    client = _FakeClient()
    runtime, _repository = _make_runtime(partial(_return_client, client))
    await runtime.startup()

    async def foreign_calls() -> tuple[type[BaseException], ...]:
        errors: list[type[BaseException]] = []
        for operation in (runtime.startup, runtime.shutdown):
            try:
                await operation()
            except BaseException as error:  # noqa: BLE001 - only safe types leave the thread.
                errors.append(type(error))
        try:
            runtime.repository()
        except BaseException as error:  # noqa: BLE001 - only safe types leave the thread.
            errors.append(type(error))
        return tuple(errors)

    error_types = await asyncio.to_thread(lambda: asyncio.run(foreign_calls()))

    assert error_types == (
        ClickHouseLifecycleError,
        ClickHouseLifecycleError,
        ClickHouseLifecycleError,
    )
    assert client.close_calls == 0
    await runtime.shutdown()


def test_concurrent_new_startups_from_distinct_loops_have_one_owner() -> None:
    """Let the active lease reject another loop before its factory runs."""
    client = _FakeClient()
    factory_entered = Event()
    factory_release = Event()
    factory_calls = 0

    async def client_factory() -> AsyncClient:
        nonlocal factory_calls
        factory_calls += 1
        factory_entered.set()
        if not factory_release.wait(timeout=_THREAD_TIMEOUT):
            message = "factory release timed out"
            raise RuntimeError(message)
        return await _return_client(client)

    async def owner(runtime: BackendRuntime) -> None:
        await runtime.startup()
        await runtime.shutdown()

    async def contender(runtime: BackendRuntime) -> str:
        with pytest.raises(ClickHouseLifecycleError) as raised:
            await runtime.startup()
        return raised.value.reason

    runtime, _repository = _make_runtime(client_factory)
    with ThreadPoolExecutor(max_workers=2) as executor:
        owner_result = executor.submit(asyncio.run, owner(runtime))
        assert factory_entered.wait(timeout=_THREAD_TIMEOUT)
        contender_result = executor.submit(asyncio.run, contender(runtime))
        try:
            assert contender_result.result(timeout=_THREAD_TIMEOUT) == "foreign_runtime"
        finally:
            factory_release.set()
        owner_result.result(timeout=_THREAD_TIMEOUT)

    assert factory_calls == 1
    assert client.close_calls == 1
    assert runtime.state is LifecycleState.CLOSED


def test_manager_lease_can_be_reused_by_later_worker_event_loop() -> None:
    """Release manager ownership before binding READY state to another loop."""
    manager_client = _FakeClient()
    worker_client = _FakeClient()
    clients = iter((manager_client, worker_client))

    async def client_factory() -> AsyncClient:
        return await _return_client(next(clients))

    async def worker_phase(runtime: BackendRuntime) -> None:
        await runtime.startup()
        await runtime.shutdown()

    runtime, _repository = _make_runtime(client_factory)

    asyncio.run(runtime.run_schema_manager(mode="validate", actor=SchemaActor.WORKER))
    asyncio.run(worker_phase(runtime))

    assert manager_client.close_calls == 1
    assert worker_client.close_calls == 1
    assert runtime.state is LifecycleState.CLOSED


@pytest.mark.asyncio
async def test_startup_cancellation_closes_partial_client_before_propagation() -> None:
    """Drain the exact partial-startup client close before cancellation escapes."""
    close_release = asyncio.Event()
    client = _FakeClient(close_release=close_release)
    barrier = _SchemaBarrierProbe(release=asyncio.Event())

    runtime, _repository = _make_runtime(
        partial(_return_client, client),
        schema_barrier=barrier,
    )
    startup = asyncio.create_task(runtime.startup())
    await _wait(barrier.entered)
    startup.cancel()
    await _wait(client.close_started)

    assert not startup.done()
    startup.cancel()
    close_release.set()
    with pytest.raises(asyncio.CancelledError):
        await startup

    assert client.close_calls == 1
    assert runtime.state is LifecycleState.NEW


@pytest.mark.asyncio
async def test_startup_cancellation_surfaces_fatal_cleanup_after_state_reset() -> None:
    """Reset the runtime after the exact cancelled-startup close fails fatally."""
    close_release = asyncio.Event()
    fatal = _FatalRuntimeError()
    client = _FakeClient(close_release=close_release, close_error=fatal)
    barrier = _SchemaBarrierProbe(release=asyncio.Event())
    runtime, _repository = _make_runtime(
        partial(_return_client, client),
        schema_barrier=barrier,
    )
    startup = asyncio.create_task(runtime.startup())
    await _wait(barrier.entered)

    startup.cancel("outer-cancellation")
    await _wait(client.close_started)
    assert not startup.done()
    close_release.set()
    with pytest.raises(_FatalRuntimeError) as raised:
        await startup

    assert raised.value is fatal
    assert client.close_calls == 1
    assert runtime.state is LifecycleState.NEW


@pytest.mark.asyncio
async def test_factory_cancellation_closes_late_client_and_restores_new() -> None:
    """Finish shielded creation and close its result after outer cancellation."""
    client = _FakeClient()
    factory_release = asyncio.Event()
    client_factory = _ClientFactoryProbe(client, release=factory_release)

    runtime, _repository = _make_runtime(client_factory)
    startup = asyncio.create_task(runtime.startup())
    await _wait(client_factory.entered)
    startup.cancel()
    factory_release.set()

    with pytest.raises(asyncio.CancelledError):
        await startup

    assert client.close_calls == 1
    assert runtime.state is LifecycleState.NEW


@pytest.mark.asyncio
async def test_cancelled_factory_task_propagates_and_restores_new() -> None:
    """Keep internal factory cancellation distinct from retryable I/O failure."""
    client_factory = _ClientFactoryProbe(_FakeClient(), error=asyncio.CancelledError())
    runtime, _repository = _make_runtime(client_factory)

    with pytest.raises(asyncio.CancelledError):
        await runtime.startup()

    assert runtime.state is LifecycleState.NEW


@pytest.mark.asyncio
async def test_failed_startup_close_cancellation_wins_and_restores_new() -> None:
    """Preserve cleanup cancellation instead of an earlier barrier error."""
    client = _FakeClient(close_error=asyncio.CancelledError())
    message = "unsafe barrier failure"
    barrier = _SchemaBarrierProbe(error=RuntimeError(message))

    runtime, _repository = _make_runtime(
        partial(_return_client, client),
        schema_barrier=barrier,
    )

    with pytest.raises(asyncio.CancelledError):
        await runtime.startup()

    assert client.close_calls == 1
    assert runtime.state is LifecycleState.NEW


@pytest.mark.asyncio
async def test_fatal_startup_failure_keeps_identity_after_cleanup() -> None:
    """Close the partial client and re-raise the same non-Exception failure."""
    client = _FakeClient(close_error=RuntimeError("unsafe close failure"))
    fatal = _FatalRuntimeError()
    barrier = _SchemaBarrierProbe(error=fatal)

    runtime, _repository = _make_runtime(
        partial(_return_client, client),
        schema_barrier=barrier,
    )

    with pytest.raises(_FatalRuntimeError) as raised:
        await runtime.startup()

    assert raised.value is fatal
    assert client.close_calls == 1
    assert runtime.state is LifecycleState.NEW


@pytest.mark.asyncio
async def test_shutdown_cancellation_drains_same_close_and_stays_closed() -> None:
    """Complete one raw close before propagating outer cancellation."""
    close_release = asyncio.Event()
    client = _FakeClient(close_release=close_release)
    runtime, _repository = _make_runtime(partial(_return_client, client))
    await runtime.startup()
    shutdown = asyncio.create_task(runtime.shutdown())
    await _wait(client.close_started)
    shutdown.cancel()

    assert not shutdown.done()
    close_release.set()
    with pytest.raises(asyncio.CancelledError):
        await shutdown

    assert client.close_calls == 1
    assert runtime.state is LifecycleState.CLOSED
    await runtime.shutdown()


@pytest.mark.asyncio
async def test_shutdown_failure_is_safe_terminal_and_not_retried() -> None:
    """Redact raw close failure while permanently releasing runtime state."""
    secret = "password=close-secret private.internal"  # noqa: S105  # pragma: allowlist secret
    client = _FakeClient(close_error=RuntimeError(secret))
    runtime, _repository = _make_runtime(partial(_return_client, client))
    await runtime.startup()

    with pytest.raises(ClickHouseBackendIOError, match="client_close_failed") as raised:
        await runtime.shutdown()

    assert secret not in str(raised.value)
    _assert_detached(raised.value)
    assert runtime.state is LifecycleState.CLOSED
    await runtime.shutdown()
    assert client.close_calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_phase", ["factory", "barrier", "close"])
async def test_schema_operation_raw_failures_are_closed_and_redacted(
    failure_phase: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Expose only code-owned manager failures without exception chains."""
    secret = "password=top-secret dsn=https://private.internal payload=task-log"  # noqa: S105  # pragma: allowlist secret
    close_error = RuntimeError(secret) if failure_phase == "close" else None
    client = _FakeClient(close_error=close_error)
    barrier_error = RuntimeError(secret) if failure_phase == "barrier" else None
    barrier = _SchemaBarrierProbe(error=barrier_error)
    factory_error = RuntimeError(secret) if failure_phase == "factory" else None
    client_factory = _ClientFactoryProbe(client, error=factory_error)

    runtime, _repository = _make_runtime(client_factory, schema_barrier=barrier)

    with pytest.raises(ClickHouseBackendIOError) as raised:
        await runtime.run_schema_manager(mode="migrate", actor=SchemaActor.MANAGER)

    rendered_traceback = "".join(traceback.format_exception(raised.value))
    assert secret not in repr(raised.value) + str(raised.value)
    assert secret not in rendered_traceback
    assert secret not in caplog.text
    assert "private.internal" not in rendered_traceback
    _assert_detached(raised.value)
    assert client.close_calls == (0 if failure_phase == "factory" else 1)
    assert runtime.state is LifecycleState.NEW


@pytest.mark.asyncio
async def test_schema_operation_rebuilds_safe_error_without_raw_context(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Keep safe codes while detaching a barrier error from raw context."""
    client = _FakeClient()
    secret = "password=context-secret dsn=https://private.internal"  # noqa: S105  # pragma: allowlist secret
    safe_error = ClickHouseSchemaError("schema", "drift")

    async def barrier(
        client: AsyncClient,
        *,
        mode: SchemaMode,
        actor: SchemaActor,
    ) -> None:
        del client, mode, actor
        safe_error.__context__ = RuntimeError(secret)
        raise safe_error

    runtime, _repository = _make_runtime(
        partial(_return_client, client),
        schema_barrier=barrier,
    )

    with pytest.raises(ClickHouseSchemaError) as raised:
        await runtime.run_schema_manager(mode="validate", actor=SchemaActor.WORKER)

    assert raised.value is not safe_error
    assert type(raised.value) is ClickHouseSchemaError
    assert (raised.value.operation, raised.value.reason) == ("schema", "drift")
    _assert_detached(raised.value)
    assert secret not in "".join(traceback.format_exception(raised.value))
    assert secret not in caplog.text
    assert client.close_calls == 1


@pytest.mark.asyncio
async def test_schema_operation_detaches_physical_drift_with_report() -> None:
    """Preserve a value-free drift report across runtime error translation."""
    client = _FakeClient()
    table = storage_layout_from_names("tasks", "results", "progress").result_table
    report = SchemaDriftReport(
        mismatch_count=1,
        locations=(SchemaDriftLocation(table, "engine"),),
    )
    internal_error = _PhysicalSchemaDriftError(report)
    barrier = _SchemaBarrierProbe(error=internal_error)

    runtime, _repository = _make_runtime(
        partial(_return_client, client),
        schema_barrier=barrier,
    )

    with pytest.raises(_PhysicalSchemaDriftError) as raised:
        await runtime.run_schema_manager(mode="validate", actor=SchemaActor.WORKER)

    assert raised.value is not internal_error
    assert raised.value.report is report
    _assert_detached(raised.value)


@pytest.mark.asyncio
async def test_schema_operation_fatal_failure_wins_after_cleanup() -> None:
    """Re-raise one fatal barrier failure after swallowing raw close failure."""
    client = _FakeClient(close_error=RuntimeError("unsafe close failure"))
    fatal = _FatalRuntimeError()
    barrier = _SchemaBarrierProbe(error=fatal)

    runtime, _repository = _make_runtime(
        partial(_return_client, client),
        schema_barrier=barrier,
    )

    with pytest.raises(_FatalRuntimeError) as raised:
        await runtime.run_schema_manager(mode="migrate", actor=SchemaActor.MANAGER)

    assert raised.value is fatal
    assert client.close_calls == 1
    assert runtime.state is LifecycleState.NEW


@pytest.mark.asyncio
async def test_schema_operation_cancellation_waits_for_client_close() -> None:
    """Finish temporary-client cleanup before propagating barrier cancellation."""
    close_release = asyncio.Event()
    client = _FakeClient(close_release=close_release)
    barrier = _SchemaBarrierProbe(release=asyncio.Event())

    runtime, _repository = _make_runtime(
        partial(_return_client, client),
        schema_barrier=barrier,
    )
    operation = asyncio.create_task(
        runtime.run_schema_manager(mode="migrate", actor=SchemaActor.MANAGER),
    )
    await _wait(barrier.entered)
    operation.cancel()
    await _wait(client.close_started)

    assert not operation.done()
    close_release.set()
    with pytest.raises(asyncio.CancelledError):
        await operation

    assert client.close_calls == 1
    assert runtime.state is LifecycleState.NEW


@pytest.mark.asyncio
async def test_schema_operation_outer_close_cancellation_does_not_retry() -> None:
    """Drain the same temporary-client close task after outer cancellation."""
    close_release = asyncio.Event()
    client = _FakeClient(close_release=close_release)
    runtime, _repository = _make_runtime(partial(_return_client, client))
    operation = asyncio.create_task(
        runtime.run_schema_manager(mode="validate", actor=SchemaActor.WORKER),
    )
    await _wait(client.close_started)
    operation.cancel()

    assert not operation.done()
    close_release.set()
    with pytest.raises(asyncio.CancelledError):
        await operation

    assert client.close_calls == 1
    assert runtime.state is LifecycleState.NEW


@pytest.mark.asyncio
async def test_schema_operation_failed_cleanup_cancellation_wins() -> None:
    """Propagate internal close cancellation instead of an earlier raw error."""
    client = _FakeClient(close_error=asyncio.CancelledError())
    message = "unsafe barrier failure"
    barrier = _SchemaBarrierProbe(error=RuntimeError(message))

    runtime, _repository = _make_runtime(
        partial(_return_client, client),
        schema_barrier=barrier,
    )

    with pytest.raises(asyncio.CancelledError):
        await runtime.run_schema_manager(mode="migrate", actor=SchemaActor.MANAGER)

    assert client.close_calls == 1
    assert runtime.state is LifecycleState.NEW


@pytest.mark.asyncio
async def test_schema_operation_factory_cancellation_closes_late_client() -> None:
    """Finish manager client creation and cleanup after caller cancellation."""
    client = _FakeClient(close_error=RuntimeError("unsafe close failure"))
    factory_release = asyncio.Event()
    client_factory = _ClientFactoryProbe(client, release=factory_release)

    runtime, _repository = _make_runtime(client_factory)
    operation = asyncio.create_task(
        runtime.run_schema_manager(mode="validate", actor=SchemaActor.WORKER),
    )
    await _wait(client_factory.entered)
    operation.cancel()
    factory_release.set()

    with pytest.raises(asyncio.CancelledError):
        await operation

    assert client.close_calls == 1
    assert runtime.state is LifecycleState.NEW
