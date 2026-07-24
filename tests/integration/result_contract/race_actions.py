"""Run deterministic public consume races over real ClickHouse repositories."""

from __future__ import annotations

from contextlib import AsyncExitStack
from typing import TYPE_CHECKING, Any

from tests.integration.result_contract.backend_actions import (
    ResultBackendHarness,
    running_backend,
)
from tests.integration.result_contract.consume_actions import (
    consume_concurrently,
    consume_while_writing_newer,
)
from tests.integration.result_contract.models import unique_namespace
from tests.integration.result_contract.observations import (
    BackendObservation,
    ConcurrentConsumeObservation,
    TargetedConsumeObservation,
)
from tests.integration.result_contract.scenario_actions import capture_get_error


if TYPE_CHECKING:
    from taskiq.result import TaskiqResult

    from tests.integration.settings import ClickHouseTestSettings


async def run_concurrent_consume(
    settings: ClickHouseTestSettings,
    database: str,
    task_id: str,
    source: TaskiqResult[Any],
) -> ConcurrentConsumeObservation:
    """Run a complete two-reader best-effort public consume scenario."""
    async with AsyncExitStack() as backend_stack:
        consumers = await _enter_consumer_pair(backend_stack, settings, database)
        await consumers[0].set_result(task_id, source)
        observed = await consume_concurrently(consumers[0], consumers[1], task_id)
        final_state = await _consume_final_state(consumers, task_id)
    return ConcurrentConsumeObservation(
        first=observed[0],
        second=observed[1],
        ready_after_consume=final_state[0],
        missing_error=final_state[1],
    )


async def run_targeted_consume(
    settings: ClickHouseTestSettings,
    database: str,
    task_id: str,
    older: TaskiqResult[Any],
    newer: TaskiqResult[Any],
) -> TargetedConsumeObservation:
    """Run the public read-A, write-B and tombstone-A interleaving."""
    async with AsyncExitStack() as backend_stack:
        consumer, writer = await _enter_targeted_pair(
            backend_stack,
            settings,
            database,
        )
        await writer.set_result(task_id, older)
        consumed = await consume_while_writing_newer(
            consumer,
            writer,
            task_id,
            newer,
        )
        latest = await _latest_state(writer, task_id)
    return TargetedConsumeObservation(consumed, latest)


async def _enter_consumer_pair(
    backend_stack: AsyncExitStack,
    settings: ClickHouseTestSettings,
    database: str,
) -> tuple[ResultBackendHarness, ResultBackendHarness]:
    namespace = unique_namespace("public-concurrent-consume")
    first = await backend_stack.enter_async_context(
        running_backend(settings, database, namespace, keep_results=False),
    )
    second = await backend_stack.enter_async_context(
        running_backend(settings, database, namespace, keep_results=False),
    )
    return first, second


async def _enter_targeted_pair(
    backend_stack: AsyncExitStack,
    settings: ClickHouseTestSettings,
    database: str,
) -> tuple[ResultBackendHarness, ResultBackendHarness]:
    namespace = unique_namespace("public-targeted-consume")
    consumer = await backend_stack.enter_async_context(
        running_backend(settings, database, namespace, keep_results=False),
    )
    writer = await backend_stack.enter_async_context(
        running_backend(settings, database, namespace, keep_results=True),
    )
    return consumer, writer


async def _consume_final_state(
    consumers: tuple[ResultBackendHarness, ResultBackendHarness],
    task_id: str,
) -> tuple[bool, Exception | None]:
    ready = await consumers[0].is_result_ready(task_id)
    missing_error = await capture_get_error(consumers[1], task_id)
    return ready, missing_error


async def _latest_state(
    backend: ResultBackendHarness,
    task_id: str,
) -> BackendObservation:
    ready = await backend.is_result_ready(task_id)
    latest = await backend.get_result(task_id, with_logs=True)
    return BackendObservation(ready, latest)
