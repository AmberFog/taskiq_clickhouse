"""Low-level deterministic gates for public best-effort consume workflows."""

from __future__ import annotations

import asyncio
from functools import partial
from typing import TYPE_CHECKING, Any

from tests.integration.result_contract.constants import CONCURRENCY_TIMEOUT_SECONDS
from tests.integration.result_contract.gateways import (
    CapturedReadGateway,
    ReadBarrierGateway,
)


if TYPE_CHECKING:
    from taskiq.result import TaskiqResult

    from tests.integration.result_contract.backend_actions import ResultBackendHarness


def install_consumer_barrier(
    backend: ResultBackendHarness,
    barrier: asyncio.Barrier,
) -> None:
    """Gate one no-log selection while retaining the real repository and client."""
    backend.install_gateway(
        partial(
            ReadBarrierGateway,
            target_query=backend.queries.no_log,
            barrier=barrier,
        ),
    )


async def consume_concurrently(
    first: ResultBackendHarness,
    second: ResultBackendHarness,
    task_id: str,
) -> tuple[TaskiqResult[Any], TaskiqResult[Any]]:
    """Force both public consumers to materialize the same generation first."""
    barrier = asyncio.Barrier(2)
    install_consumer_barrier(first, barrier)
    install_consumer_barrier(second, barrier)
    tasks = (
        asyncio.create_task(first.get_result(task_id, with_logs=False)),
        asyncio.create_task(second.get_result(task_id, with_logs=False)),
    )
    try:
        async with asyncio.timeout(CONCURRENCY_TIMEOUT_SECONDS):
            observed = await asyncio.gather(*tasks)
    except BaseException:
        for consumer_task in tasks:
            consumer_task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise
    return observed[0], observed[1]


async def consume_while_writing_newer(
    consumer: ResultBackendHarness,
    writer: ResultBackendHarness,
    task_id: str,
    newer: TaskiqResult[Any],
) -> TaskiqResult[Any]:
    """Capture A, write B, then let public consumption tombstone only A."""
    captured = asyncio.Event()
    release = asyncio.Event()
    consumer.install_gateway(
        partial(
            CapturedReadGateway,
            target_query=consumer.queries.with_log,
            captured=captured,
            release=release,
        ),
    )
    consume_task = asyncio.create_task(
        consumer.get_result(task_id, with_logs=True),
    )
    try:
        async with asyncio.timeout(CONCURRENCY_TIMEOUT_SECONDS):
            await captured.wait()
            await writer.set_result(task_id, newer)
            release.set()
            return await consume_task
    except BaseException:
        release.set()
        consume_task.cancel()
        await asyncio.gather(consume_task, return_exceptions=True)
        raise
    finally:
        release.set()
