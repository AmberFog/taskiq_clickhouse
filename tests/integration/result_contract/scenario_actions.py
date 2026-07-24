"""High-level public result workflows shared by integration scenarios."""

from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack
from typing import TYPE_CHECKING

from tests.integration.result_contract.backend_actions import (
    ResultBackendHarness,
    running_backend,
)
from tests.integration.result_contract.models import unique_namespace
from tests.integration.result_contract.observations import (
    BackendObservation,
    NamespaceObservation,
    RewriteObservation,
)


if TYPE_CHECKING:
    from taskiq.result import TaskiqResult

    from tests.integration.settings import ClickHouseTestSettings


async def capture_get_error(
    backend: ResultBackendHarness,
    task_id: str,
) -> Exception | None:
    """Return one ordinary public get failure without swallowing cancellation."""
    try:
        await backend.get_result(task_id)
    except Exception as task_error:  # noqa: BLE001 - test records the exact public type.
        return task_error
    return None


async def run_consume_rewrite(
    settings: ClickHouseTestSettings,
    database: str,
    task_id: str,
    initial: TaskiqResult[object],
    fresh: TaskiqResult[object],
) -> RewriteObservation:
    """Consume one generation, observe absence and write a fresh generation."""
    async with running_backend(
        settings,
        database,
        unique_namespace("public-rewrite"),
        keep_results=False,
    ) as backend:
        consumed_state = await _consume_initial(backend, task_id, initial)
        fresh_state = await _write_fresh(backend, task_id, fresh)
    return RewriteObservation(
        consumed=consumed_state[0],
        ready_after_consume=consumed_state[1],
        missing_error=consumed_state[2],
        fresh=fresh_state,
    )


async def run_namespace_isolation(
    settings: ClickHouseTestSettings,
    database: str,
    task_id: str,
    first_source: TaskiqResult[object],
    second_source: TaskiqResult[object],
) -> NamespaceObservation:
    """Write and read one shared task id through two real namespaces."""
    async with AsyncExitStack() as backend_stack:
        first_backend = await _enter_backend(
            backend_stack,
            settings,
            database,
            "namespace-a",
        )
        second_backend = await _enter_backend(
            backend_stack,
            settings,
            database,
            "namespace-b",
        )
        observations = await asyncio.gather(
            _round_trip(first_backend, task_id, first_source),
            _round_trip(second_backend, task_id, second_source),
        )
    return NamespaceObservation(observations[0], observations[1])


async def _enter_backend(
    backend_stack: AsyncExitStack,
    settings: ClickHouseTestSettings,
    database: str,
    prefix: str,
) -> ResultBackendHarness:
    return await backend_stack.enter_async_context(
        running_backend(
            settings,
            database,
            unique_namespace(prefix),
            keep_results=True,
        ),
    )


async def _consume_initial(
    backend: ResultBackendHarness,
    task_id: str,
    source: TaskiqResult[object],
) -> tuple[TaskiqResult[object], bool, Exception | None]:
    await backend.set_result(task_id, source)
    consumed = await backend.get_result(task_id, with_logs=True)
    ready = await backend.is_result_ready(task_id)
    missing_error = await capture_get_error(backend, task_id)
    return consumed, ready, missing_error


async def _write_fresh(
    backend: ResultBackendHarness,
    task_id: str,
    source: TaskiqResult[object],
) -> BackendObservation:
    await backend.set_result(task_id, source)
    ready = await backend.is_result_ready(task_id)
    observed = await backend.get_result(task_id, with_logs=True)
    return BackendObservation(ready, observed)


async def _round_trip(
    backend: ResultBackendHarness,
    task_id: str,
    source: TaskiqResult[object],
) -> BackendObservation:
    await backend.set_result(task_id, source)
    ready = await backend.is_result_ready(task_id)
    observed = await backend.get_result(task_id, with_logs=True)
    return BackendObservation(ready, observed)
