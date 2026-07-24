"""Public backend harnesses with explicit runtime and storage ownership."""

from __future__ import annotations

import asyncio
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from functools import partial
from threading import Barrier, Lock
from typing import TYPE_CHECKING, Any, cast
from uuid import UUID

from taskiq_clickhouse._backend_runtime import BackendRuntime, RuntimeDependencies
from taskiq_clickhouse._storage.layout import storage_layout_from_names
from taskiq_clickhouse._storage.repository import StorageRepository
from taskiq_clickhouse._storage_policy import NamespaceKey, RetentionPolicy, StoragePolicy
from taskiq_clickhouse.backend import ClickHouseResultBackend
from tests.result_contract.models import error_result


if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Coroutine, Mapping

    from clickhouse_connect.driver.asyncclient import AsyncClient
    from taskiq.abc.serializer import TaskiqSerializer
    from taskiq.result import TaskiqResult

    from taskiq_clickhouse._clickhouse.contracts import ReadWriteGateway
    from taskiq_clickhouse._clickhouse.request import InsertRequest
    from taskiq_clickhouse._types import SchemaActor, SchemaMode


_NOW = datetime(2026, 7, 16, 12, 0, tzinfo=UTC)
_VISIBLE = _NOW + timedelta(hours=1)
_PURGE = _NOW + timedelta(days=1)
_GENERATION_ID = UUID("11111111-1111-4111-8111-111111111111")
_DAY = timedelta(days=1)
_WEEK = timedelta(days=7)
_MICROSECOND = timedelta(microseconds=1)
_RESULT_TTL_US = _DAY // _MICROSECOND
_PURGE_TTL_US = _WEEK // _MICROSECOND
_DATABASE = "tasks"
_NAMESPACE = "default"
_RESULT_TABLE = "taskiq_clickhouse_results"
_PROGRESS_TABLE = "taskiq_clickhouse_progress"


class CaptureGateway:
    """Provide fixed server time and capture every native result insert."""

    def __init__(self) -> None:
        """Create one thread-safe observation collector."""
        self._lock = Lock()
        self.inserts: list[InsertRequest] = []
        self.query_calls = 0

    async def query_rows(
        self,
        query: str,
        *,
        query_parameters: Mapping[str, object] | None = None,
        settings: Mapping[str, object] | None = None,
        column_formats: Mapping[str, str] | None = None,
    ) -> tuple[tuple[object, ...], ...]:
        """Return an empty-history allocator observation."""
        del query, query_parameters, settings, column_formats
        with self._lock:
            self.query_calls += 1
        return ((_NOW, None, None),)

    async def insert_rows(self, request: InsertRequest) -> None:
        """Capture one fully encoded native insert."""
        with self._lock:
            self.inserts.append(request)


class ScriptedGateway:
    """Replay exact adapter outcomes through a public repository."""

    def __init__(
        self,
        *,
        query_events: list[object],
        insert_events: list[BaseException | None] | None = None,
    ) -> None:
        """Retain deterministic I/O events and observations."""
        self._query_events = deque(query_events)
        self._insert_events = deque(() if insert_events is None else insert_events)
        self.query_calls = 0
        self.inserts: list[InsertRequest] = []

    async def query_rows(
        self,
        query: str,
        *,
        query_parameters: Mapping[str, object] | None = None,
        settings: Mapping[str, object] | None = None,
        column_formats: Mapping[str, str] | None = None,
    ) -> tuple[tuple[object, ...], ...]:
        """Return or raise the next exact query event."""
        del query, query_parameters, settings, column_formats
        self.query_calls += 1
        if not self._query_events:
            message = "unexpected query"
            raise AssertionError(message)
        event = self._query_events.popleft()
        if isinstance(event, BaseException):
            raise event
        return cast("tuple[tuple[object, ...], ...]", event)

    async def insert_rows(self, request: InsertRequest) -> None:
        """Capture an insert, then resolve its scripted outcome."""
        self.inserts.append(request)
        if not self._insert_events:
            message = "unexpected insert"
            raise AssertionError(message)
        event = self._insert_events.popleft()
        if event is not None:
            raise event


class _NoopClient:
    """Minimal owned client required by the real runtime collaborator."""

    async def close(self) -> None:
        """Complete runtime cleanup without an external resource."""


@asynccontextmanager
async def running_backend(
    *,
    gateway: ReadWriteGateway,
    serializer: TaskiqSerializer | None = None,
    serializer_id: str | None = None,
    keep_results: bool = True,
) -> AsyncIterator[ClickHouseResultBackend[Any]]:
    """Start and close a public facade over a real runtime and repository."""
    backend = ClickHouseResultBackend[Any](
        host="localhost",
        database=_DATABASE,
        secure=False,
        result_ttl=_DAY,
        purge_ttl=_WEEK,
        keep_results=keep_results,
        serializer=serializer,
        serializer_id=serializer_id,
    )
    repository = _repository(gateway)
    backend._runtime = _runtime(repository)  # noqa: SLF001 - single test composition seam.
    await backend.startup()
    try:
        yield backend
    finally:
        await backend.shutdown()


def result_row(
    result_payload: bytes,
    *,
    log_payload: bytes | None = None,
) -> tuple[tuple[object, ...], ...]:
    """Build one visible latest result projection with optional log bytes."""
    row: tuple[object, ...] = (
        _NOW,
        _NOW,
        _GENERATION_ID,
        0,
        _VISIBLE,
        _PURGE,
        result_payload,
    )
    if log_payload is not None:
        row = (*row, log_payload)
    return (row,)


def readiness_row() -> tuple[tuple[object, ...], ...]:
    """Build one ready metadata-only latest-state projection."""
    return ((_NOW, _NOW, _GENERATION_ID, 0, _VISIBLE, _PURGE),)


def allocation_row() -> tuple[tuple[object, ...], ...]:
    """Build one empty-history generation-allocation observation."""
    return ((_NOW, None, None),)


def run_parallel_error_writes(gateway: CaptureGateway, *, count: int) -> None:
    """Call public set_result on independent backends and event loops."""
    start = Barrier(count)
    write_one = partial(_write_error_in_new_loop, gateway=gateway, start=start)
    with ThreadPoolExecutor(max_workers=count) as executor:
        tuple(executor.map(write_one, range(count)))


async def set_result_from_foreign_loop(
    backend: ClickHouseResultBackend[Any],
    task_result: TaskiqResult[Any],
) -> None:
    """Invoke public set_result from a same-PID foreign event loop."""
    operation: Callable[[], Coroutine[Any, Any, None]] = partial(
        backend.set_result,
        "foreign-loop",
        task_result,
    )
    await asyncio.to_thread(_run_in_new_loop, operation)


def _repository(gateway: ReadWriteGateway) -> StorageRepository:
    layout = storage_layout_from_names(_DATABASE, _RESULT_TABLE, _PROGRESS_TABLE)
    return StorageRepository(
        gateway=gateway,
        layout=layout,
        policy=StoragePolicy(
            namespace=NamespaceKey(_NAMESPACE),
            retention=RetentionPolicy(_RESULT_TTL_US, _PURGE_TTL_US),
        ),
    )


def _runtime(repository: StorageRepository) -> BackendRuntime:
    client = _NoopClient()

    async def client_factory() -> AsyncClient:
        return cast("AsyncClient", client)

    async def schema_barrier(
        client: AsyncClient,
        *,
        mode: SchemaMode,
        actor: SchemaActor,
    ) -> None:
        del client, mode, actor

    def repository_factory(observed_client: AsyncClient) -> StorageRepository:
        del observed_client
        return repository

    dependencies = RuntimeDependencies(
        client_factory=client_factory,
        schema_runner=schema_barrier,
        repository_factory=repository_factory,
    )
    return BackendRuntime(dependencies, schema_mode="migrate")


def _write_error_in_new_loop(
    index: int,
    *,
    gateway: CaptureGateway,
    start: Barrier,
) -> None:
    start.wait()
    asyncio.run(_write_error(index, gateway))


async def _write_error(index: int, gateway: CaptureGateway) -> None:
    async with running_backend(gateway=gateway) as backend:
        await backend.set_result(f"parallel-{index}", error_result(index))


def _run_in_new_loop(
    awaitable_factory: Callable[[], Coroutine[Any, Any, None]],
) -> None:
    asyncio.run(awaitable_factory())
