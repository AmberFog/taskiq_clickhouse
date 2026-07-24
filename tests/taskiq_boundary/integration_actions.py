"""Real ClickHouse harness for Taskiq receiver-boundary tests."""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import taskiq

from taskiq_clickhouse.backend import ClickHouseResultBackend
from tests.taskiq_boundary import (
    constants as boundary_constants,
    models as boundary_models,
)


if TYPE_CHECKING:
    from collections import abc

    from tests.integration import settings as integration_settings


@dataclass(frozen=True, slots=True)
class TaskGates:
    """Own deterministic start and release barriers for one task."""

    started: asyncio.Event
    release: asyncio.Event


@dataclass(slots=True)
class TaskiqReceiverHarness:
    """Own a loop-local backend, broker, task gates and typed senders."""

    backend: ClickHouseResultBackend[Any]
    broker: taskiq.InMemoryBroker
    gates: TaskGates
    success_task: taskiq.AsyncTaskiqDecoratedTask[
        [int],
        abc.Coroutine[Any, Any, boundary_models.TypedReceiverResult],
    ]
    error_task: taskiq.AsyncTaskiqDecoratedTask[[], abc.Coroutine[Any, Any, None]]

    async def send_success(self, answer: int) -> taskiq.AsyncTaskiqTask[boundary_models.TypedReceiverResult]:
        """Send the gated typed success task."""
        return await self.success_task.kiq(answer)

    async def send_error(self) -> taskiq.AsyncTaskiqTask[None]:
        """Send the custom-error task."""
        return await self.error_task.kiq()


@contextlib.asynccontextmanager
async def taskiq_receiver_harness(
    settings: integration_settings.ClickHouseTestSettings,
    database: str,
) -> abc.AsyncIterator[TaskiqReceiverHarness]:
    """Start a backend explicitly because InMemoryBroker does not own it."""
    backend = _integration_backend(settings, database)
    broker = taskiq.InMemoryBroker(await_inplace=False).with_result_backend(backend)
    gates = TaskGates(asyncio.Event(), asyncio.Event())
    async with contextlib.AsyncExitStack() as cleanup:
        # LIFO cleanup: drain receiver tasks, stop the broker, then close storage.
        cleanup.push_async_callback(backend.shutdown)
        cleanup.push_async_callback(broker.shutdown)
        cleanup.push_async_callback(broker.wait_all)

        @broker.task(
            task_name=boundary_constants.SUCCESS_TASK_NAME,
            source=boundary_constants.SUCCESS_LABEL_SOURCE,
            attempt=boundary_constants.SUCCESS_LABEL_ATTEMPT,
        )
        async def typed_success(  # noqa: WPS430 - task closure owns loop-local gates.
            answer: int,
        ) -> boundary_models.TypedReceiverResult:
            gates.started.set()
            await gates.release.wait()
            return boundary_models.TypedReceiverResult(answer=answer)

        @broker.task(
            task_name=boundary_constants.ERROR_TASK_NAME,
            source=boundary_constants.SUCCESS_LABEL_SOURCE,
            attempt=boundary_constants.SUCCESS_LABEL_ATTEMPT,
        )
        async def typed_error() -> None:  # noqa: WPS430 - task closure belongs to its broker.
            cause = ValueError(boundary_constants.TASK_ERROR_CAUSE)
            raise boundary_models.ReceiverTaskError(boundary_constants.TASK_ERROR_MESSAGE) from cause

        await backend.startup()
        try:
            yield TaskiqReceiverHarness(
                backend=backend,
                broker=broker,
                gates=gates,
                success_task=typed_success,
                error_task=typed_error,
            )
        finally:
            gates.release.set()


async def observe_typed_success(
    harness: TaskiqReceiverHarness,
    answer: int,
) -> boundary_models.TypedSuccessObservation:
    """Run the deterministic readiness, wait and typed-get workflow."""
    task = await harness.send_success(answer)
    async with asyncio.timeout(boundary_constants.BOUNDARY_TIMEOUT_SECONDS):
        await harness.gates.started.wait()
    ready_before_completion = await task.is_ready()
    harness.gates.release.set()
    waited, fetched = await _finish_typed_success(harness, task)
    return boundary_models.TypedSuccessObservation(ready_before_completion, waited, fetched)


async def _finish_typed_success(
    harness: TaskiqReceiverHarness,
    task: taskiq.AsyncTaskiqTask[boundary_models.TypedReceiverResult],
) -> tuple[
    taskiq.TaskiqResult[boundary_models.TypedReceiverResult],
    taskiq.TaskiqResult[boundary_models.TypedReceiverResult],
]:
    """Wait for storage acknowledgement, then use wait and get APIs."""
    async with asyncio.timeout(boundary_constants.BOUNDARY_TIMEOUT_SECONDS):
        await harness.broker.wait_all()
    waited = await task.wait_result(
        check_interval=0,
        timeout=boundary_constants.BOUNDARY_TIMEOUT_SECONDS,
    )
    fetched = await task.get_result(with_logs=True)
    return waited, fetched


async def observe_task_error(
    harness: TaskiqReceiverHarness,
) -> taskiq.TaskiqResult[None]:
    """Run one receiver error task and return its stored result."""
    task = await harness.send_error()
    async with asyncio.timeout(boundary_constants.BOUNDARY_TIMEOUT_SECONDS):
        await harness.broker.wait_all()
    return await task.wait_result(
        check_interval=0,
        timeout=boundary_constants.BOUNDARY_TIMEOUT_SECONDS,
    )


def _integration_backend(
    settings: integration_settings.ClickHouseTestSettings,
    database: str,
) -> ClickHouseResultBackend[Any]:
    host = "localhost" if ":" in settings.host else settings.host
    return ClickHouseResultBackend(
        host=host,
        port=settings.port,
        username=settings.username,
        password=settings.password,
        database=database,
        secure=False,
        result_ttl=boundary_constants.RESULT_TTL,
        purge_ttl=boundary_constants.PURGE_TTL,
        namespace="taskiq-boundary",
        keep_results=True,
    )
