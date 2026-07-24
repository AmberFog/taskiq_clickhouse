"""Exercise progress through the public backend and real Taskiq dependency boundary."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING, Any, cast

import pytest
from taskiq import InMemoryBroker, TaskiqDepends
from taskiq.depends.progress_tracker import ProgressTracker, TaskProgress, TaskState

from taskiq_clickhouse._backend_composition import BackendComponents
from taskiq_clickhouse._backend_runtime import BackendRuntime, RuntimeDependencies
from taskiq_clickhouse._progress_serialization import ProgressCodec
from taskiq_clickhouse._serialization import ResultCodec
import taskiq_clickhouse.backend as backend_module
from taskiq_clickhouse.backend import ClickHouseResultBackend
from taskiq_clickhouse.exceptions import ClickHouseLifecycleError


if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from clickhouse_connect.driver.asyncclient import AsyncClient
    from taskiq.abc.serializer import TaskiqSerializer

    from taskiq_clickhouse._config_models import BackendConfig
    from taskiq_clickhouse._storage.repository import StorageRepository
    from taskiq_clickhouse._types import SchemaActor, SchemaMode


_BOUNDARY_TIMEOUT_SECONDS = 2.0
_RESULT_TTL = timedelta(hours=1)
_PURGE_TTL = timedelta(days=1)


@dataclass(frozen=True, slots=True)
class _StoredProgress:
    """Minimal persisted projection consumed by progress orchestration."""

    progress_payload: bytes


@dataclass(slots=True)
class _MemoryStore:
    """Behavior-shaped store for public facade and Taskiq boundary tests."""

    progress_by_task: dict[str, bytes] = field(default_factory=dict)
    progress_writes: list[str] = field(default_factory=list)
    progress_reads: list[str] = field(default_factory=list)
    result_writes: list[str] = field(default_factory=list)

    async def write_progress(self, task_id: str, progress_payload: bytes) -> object:
        """Persist the latest opaque progress payload for one task."""
        self.progress_writes.append(task_id)
        self.progress_by_task[task_id] = progress_payload
        return progress_payload

    async def read_progress(self, task_id: str) -> _StoredProgress | None:
        """Return the latest payload without consuming it."""
        self.progress_reads.append(task_id)
        progress_payload = self.progress_by_task.get(task_id)
        if progress_payload is None:
            return None
        return _StoredProgress(progress_payload)

    async def write_result(
        self,
        task_id: str,
        result_payload: bytes,
        log_payload: bytes,
    ) -> object:
        """Observe Taskiq receiver result persistence independently of progress."""
        del result_payload, log_payload
        self.result_writes.append(task_id)
        return task_id


@dataclass(slots=True)
class _OwnedClient:
    """Observe exactly one runtime cleanup for the composed backend."""

    close_calls: int = 0

    async def close(self) -> None:
        """Record the owned runtime close."""
        self.close_calls += 1


@dataclass(slots=True)
class _Composition:
    """Build real runtime/codecs over the behavior-shaped store."""

    store: _MemoryStore = field(default_factory=_MemoryStore)
    client: _OwnedClient = field(default_factory=_OwnedClient)

    def __call__(
        self,
        config: BackendConfig,
        serializer: TaskiqSerializer,
    ) -> BackendComponents:
        """Compose one runtime with no external I/O."""

        async def client_factory() -> AsyncClient:
            return cast("AsyncClient", self.client)

        async def schema_barrier(
            client: AsyncClient,
            *,
            mode: SchemaMode,
            actor: SchemaActor,
        ) -> None:
            del client, mode, actor

        def repository_factory(client: AsyncClient) -> StorageRepository:
            del client
            return cast("StorageRepository", self.store)

        runtime = BackendRuntime(
            RuntimeDependencies(
                client_factory=client_factory,
                schema_runner=schema_barrier,
                repository_factory=repository_factory,
            ),
            schema_mode=config.storage.schema_mode,
        )
        return BackendComponents(
            runtime=runtime,
            result_codec=ResultCodec(serializer),
            progress_codec=ProgressCodec(serializer),
            keep_results=config.storage.keep_results,
        )


def _backend(monkeypatch: pytest.MonkeyPatch, composition: _Composition) -> ClickHouseResultBackend[Any]:
    monkeypatch.setattr(backend_module, "compose_backend", composition)
    return ClickHouseResultBackend(
        host="localhost",
        database="tasks",
        secure=False,
        result_ttl=_RESULT_TTL,
        purge_ttl=_PURGE_TTL,
        namespace="progress-taskiq-boundary",
    )


@asynccontextmanager
async def _running_backend(
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[tuple[ClickHouseResultBackend[Any], _Composition]]:
    composition = _Composition()
    backend = _backend(monkeypatch, composition)
    await backend.startup()
    try:
        yield backend, composition
    finally:
        await backend.shutdown()


@pytest.mark.asyncio
async def test_public_progress_facade_requires_ready_and_remains_non_consuming(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cross NEW rejection, READY delegation and repeated public reads."""
    composition = _Composition()
    backend = _backend(monkeypatch, composition)
    expected = TaskProgress(state=TaskState.STARTED, meta={"completed": 2, "total": 5})

    with pytest.raises(ClickHouseLifecycleError, match="not_ready"):
        await backend.set_progress("task", expected)
    with pytest.raises(ClickHouseLifecycleError, match="not_ready"):
        await backend.get_progress("task")
    assert composition.store.progress_writes == []
    assert composition.store.progress_reads == []

    await backend.startup()
    try:
        assert await backend.get_progress("missing") is None
        await backend.set_progress("task", expected)
        first = await backend.get_progress("task")
        second = await backend.get_progress("task")
    finally:
        await backend.shutdown()

    assert first == expected
    assert second == expected
    assert composition.store.progress_writes == ["task"]
    assert composition.store.progress_reads == ["missing", "task", "task"]
    assert composition.client.close_calls == 1


@pytest.mark.asyncio
async def test_real_taskiq_progress_tracker_uses_context_broker_and_backend(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Resolve ProgressTracker in a receiver task and preserve inherited meta."""
    async with _running_backend(monkeypatch) as (backend, composition):
        broker = InMemoryBroker(await_inplace=False).with_result_backend(backend)

        @broker.task(task_name="progress-tracker-boundary")
        async def tracked_task(
            tracker: ProgressTracker[dict[str, int]] = TaskiqDepends(),  # noqa: B008
        ) -> str:
            await tracker.set_progress(TaskState.STARTED, {"completed": 1, "total": 3})
            await tracker.set_progress("CUSTOM")
            return "done"

        try:
            task = await tracked_task.kiq()
            async with asyncio.timeout(_BOUNDARY_TIMEOUT_SECONDS):
                await broker.wait_all()
            first = await backend.get_progress(task.task_id)
            second = await backend.get_progress(task.task_id)
        finally:
            await broker.shutdown()

    assert first is not None
    assert first == second
    assert first.state == "CUSTOM"
    assert first.meta == {"completed": 1, "total": 3}
    assert composition.store.progress_writes == [task.task_id, task.task_id]
    assert composition.store.result_writes == [task.task_id]
    assert composition.client.close_calls == 1
