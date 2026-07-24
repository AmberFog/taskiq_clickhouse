"""Single owner of process-local backend lifecycle and storage state."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, NoReturn, cast

from taskiq_clickhouse._backend_ports import BackendRepository
from taskiq_clickhouse._client_lifecycle import ClientFactory, OwnedClient
from taskiq_clickhouse._lifecycle import SchemaBarrier, SchemaOperation
from taskiq_clickhouse._lifecycle_lease import LifecycleLease, RuntimeIdentity
from taskiq_clickhouse._types import SchemaActor, SchemaMode
from taskiq_clickhouse.exceptions import (
    ClickHouseLifecycleError,
    ClickHouseResultBackendError,
    rebuild_public_error,
)


if TYPE_CHECKING:
    from clickhouse_connect.driver.asyncclient import AsyncClient


_BACKEND_OPERATION = "backend"
_CLOSED_REASON = "closed"
_FOREIGN_RUNTIME_REASON = "foreign_runtime"
_NOT_READY_REASON = "not_ready"
_RUNTIME_INIT_REASON = "runtime_init_failed"

RepositoryFactory = Callable[["AsyncClient"], BackendRepository]


class LifecycleState(StrEnum):
    """Complete set of legal backend lifecycle phases."""

    NEW = "NEW"
    STARTING = "STARTING"
    READY = "READY"
    CLOSED = "CLOSED"


@dataclass(frozen=True, slots=True, repr=False)
class StartupFailure:
    """Safe replayable outcome for callers waiting on one startup attempt."""

    error: ClickHouseResultBackendError

    @classmethod
    def capture(
        cls,
        error: ClickHouseResultBackendError,
    ) -> StartupFailure:
        """Freeze a structured public error without retaining a traceback."""
        return cls(rebuild_public_error(error))

    def rebuild(self) -> ClickHouseResultBackendError:
        """Create a fresh public exception for one concurrent waiter."""
        return rebuild_public_error(self.error)


@dataclass(frozen=True, slots=True, repr=False)
class RuntimeDependencies:
    """Explicit construction-time dependencies of one backend runtime."""

    client_factory: ClientFactory
    schema_runner: SchemaBarrier
    repository_factory: RepositoryFactory


@dataclass(slots=True, repr=False)
class _RuntimeState:
    """Mutable lifecycle values owned exclusively by ``BackendRuntime``."""

    state: LifecycleState = LifecycleState.NEW
    identity: RuntimeIdentity | None = None
    client: AsyncClient | None = None
    repository: BackendRepository | None = None
    startup_failure: StartupFailure | None = None


class BackendRuntime:  # noqa: WPS214 - one cohesive lifecycle state-machine owner.
    """Own every lifecycle transition, client and READY repository."""

    __slots__ = ("_dependencies", "_lease", "_runtime_state", "_schema_mode")

    def __init__(
        self,
        dependencies: RuntimeDependencies,
        *,
        schema_mode: SchemaMode,
    ) -> None:
        """Create a side-effect-free runtime in the NEW state."""
        self._dependencies = dependencies
        self._schema_mode = schema_mode
        self._lease = LifecycleLease()
        self._runtime_state = _RuntimeState()

    @property
    def state(self) -> LifecycleState:
        """Expose a read-only lifecycle observation to the owning facade."""
        return self._runtime_state.state

    @property
    def is_new(self) -> bool:
        """Return whether no runtime or manager operation has claimed startup."""
        return self._runtime_state.state is LifecycleState.NEW

    async def startup(self) -> None:
        """Create one owned client and cross the complete worker barrier."""
        waited_for_startup = self._observe_starting()
        async with self._lease.hold(_BACKEND_OPERATION):
            if self._runtime_state.state is LifecycleState.READY:
                self._require_owner(RuntimeIdentity.current())
                return
            if self._runtime_state.state is LifecycleState.CLOSED:
                raise _lifecycle_error(_CLOSED_REASON)
            replay = self._replay_failure(waited_for_startup=waited_for_startup)
            if replay is not None:
                raise replay from None
            await self._start(RuntimeIdentity.current())

    async def shutdown(self) -> None:
        """Close the runtime client once and make this runtime terminal."""
        self._require_bound_caller()
        async with self._lease.hold(_BACKEND_OPERATION):
            state = self._runtime_state.state
            if state is LifecycleState.CLOSED:
                return
            if state is LifecycleState.NEW:
                self._mark_closed()
                return
            self._require_owner(RuntimeIdentity.current())
            client = cast("AsyncClient", self._runtime_state.client)
            close_error = await self._capture_terminal_close(client)
            self._mark_closed()
            if close_error is None:
                return
            raise close_error

    def repository(self) -> BackendRepository:
        """Return the READY repository only to its process and loop owner."""
        if self._runtime_state.state is not LifecycleState.READY:
            raise _lifecycle_error(_NOT_READY_REASON)
        self._require_owner(RuntimeIdentity.current())
        return cast("BackendRepository", self._runtime_state.repository)

    async def run_schema_manager(
        self,
        *,
        mode: SchemaMode,
        actor: SchemaActor,
    ) -> None:
        """Run one temporary-client schema barrier while remaining NEW."""
        operation = SchemaOperation()
        async with self._lease.hold("schema_manager"):
            if self._runtime_state.state is not LifecycleState.NEW:
                raise operation.backend_not_new_error()
            owned_client = OwnedClient()
            client = await owned_client.open(self._dependencies.client_factory)
            try:
                operation_error = await operation.capture(
                    self._dependencies.schema_runner,
                    client,
                    mode=mode,
                    actor=actor,
                )
            except asyncio.CancelledError:
                await owned_client.close_after_cancel(client)
                raise
            if operation_error is None:
                await owned_client.close(client)
                return
            await owned_client.close_after_failure(client)
            raise operation.translate_error(operation_error) from None

    async def _start(self, identity: RuntimeIdentity) -> None:
        self._runtime_state.startup_failure = None
        self._runtime_state.identity = identity
        self._runtime_state.state = LifecycleState.STARTING
        owned_client = OwnedClient(_BACKEND_OPERATION)
        client: AsyncClient | None = None
        operation_error: BaseException | None = None
        try:
            client = await owned_client.open(self._dependencies.client_factory)
            operation_error = await SchemaOperation(_BACKEND_OPERATION).capture(
                self._dependencies.schema_runner,
                client,
                mode=self._schema_mode,
                actor=SchemaActor.WORKER,
            )
        except asyncio.CancelledError:
            await self._finish_cancelled_startup(
                owned_client,
                client,
            )
            raise
        except BaseException as error:  # noqa: BLE001 - fatal failures survive owned cleanup.
            operation_error = error
        if operation_error is not None:
            await self._fail_startup(owned_client, client, operation_error)
        repository_outcome = self._capture_repository(client)
        if isinstance(repository_outcome, BaseException):
            await self._fail_startup(owned_client, client, repository_outcome)
        self._runtime_state.client = client
        self._runtime_state.repository = repository_outcome
        self._runtime_state.state = LifecycleState.READY

    def _capture_repository(
        self,
        client: AsyncClient | None,
    ) -> BackendRepository | BaseException:
        """Build a repository or detach a failure for owned cleanup."""
        if client is None:
            return SchemaOperation(_BACKEND_OPERATION).backend_error(_RUNTIME_INIT_REASON)
        try:
            return self._dependencies.repository_factory(client)
        except (TypeError, ValueError):
            return SchemaOperation(_BACKEND_OPERATION).backend_error(_RUNTIME_INIT_REASON)
        except BaseException as error:  # noqa: BLE001 - fatal failures survive owned cleanup.
            return error

    async def _fail_startup(
        self,
        owned_client: OwnedClient,
        client: AsyncClient | None,
        operation_error: BaseException,
    ) -> NoReturn:
        try:
            if client is not None:
                await owned_client.close_after_failure(client)
        except BaseException:  # State rollback must cover fatal cleanup failures.
            self._reset_failed_startup()
            raise
        self._reset_failed_startup()
        translated = SchemaOperation(_BACKEND_OPERATION).translate_error(operation_error)
        if isinstance(translated, ClickHouseResultBackendError):
            self._runtime_state.startup_failure = StartupFailure.capture(translated)
        raise translated from None

    async def _finish_cancelled_startup(
        self,
        owned_client: OwnedClient,
        client: AsyncClient | None,
    ) -> None:
        if client is None:
            self._reset_failed_startup()
            return
        try:
            await owned_client.close_after_cancel(client)
        except BaseException:  # State rollback must cover fatal cleanup failures.
            self._reset_failed_startup()
            raise
        self._reset_failed_startup()

    async def _capture_terminal_close(self, client: AsyncClient) -> BaseException | None:
        try:
            await OwnedClient(_BACKEND_OPERATION).close(client)
        except BaseException as error:  # noqa: BLE001 - state becomes terminal after cleanup.
            return error
        return None

    def _observe_starting(self) -> bool:
        state = self._runtime_state.state
        if state in {LifecycleState.STARTING, LifecycleState.READY}:
            self._require_owner(RuntimeIdentity.current())
        return state is LifecycleState.STARTING

    def _replay_failure(
        self,
        *,
        waited_for_startup: bool,
    ) -> ClickHouseResultBackendError | None:
        failure = self._runtime_state.startup_failure
        if not waited_for_startup or failure is None:
            return None
        return failure.rebuild()

    def _require_bound_caller(self) -> None:
        if self._runtime_state.state in {LifecycleState.STARTING, LifecycleState.READY}:
            self._require_owner(RuntimeIdentity.current())

    def _require_owner(self, candidate: RuntimeIdentity) -> None:
        owner = cast("RuntimeIdentity", self._runtime_state.identity)
        if not owner.matches(candidate):
            raise _lifecycle_error(_FOREIGN_RUNTIME_REASON)

    def _reset_failed_startup(self) -> None:
        self._runtime_state.client = None
        self._runtime_state.repository = None
        self._runtime_state.identity = None
        self._runtime_state.state = LifecycleState.NEW

    def _mark_closed(self) -> None:
        self._runtime_state.client = None
        self._runtime_state.repository = None
        self._runtime_state.identity = None
        self._runtime_state.state = LifecycleState.CLOSED


def _lifecycle_error(reason: str) -> ClickHouseLifecycleError:
    return ClickHouseLifecycleError(_BACKEND_OPERATION, reason)
