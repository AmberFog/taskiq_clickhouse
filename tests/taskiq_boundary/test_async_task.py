"""Verify Taskiq wrapper behavior around public backend failures."""

import asyncio
from typing import Any

import pytest
from taskiq import AsyncTaskiqTask, ResultGetError, ResultIsReadyError

from taskiq_clickhouse.backend import ClickHouseResultBackend
from taskiq_clickhouse.exceptions import ClickHouseLifecycleError
from tests.taskiq_boundary.assertions import (
    assert_not_ready_error,
    assert_wrapped_not_ready,
)
from tests.taskiq_boundary.constants import WRAPPER_TASK_ID
from tests.taskiq_boundary.doubles import CancellationResultBackend


pytestmark = pytest.mark.asyncio


async def test_direct_backend_exposes_lifecycle_error(
    unstarted_clickhouse_backend: ClickHouseResultBackend[Any],
) -> None:
    """Expose package lifecycle errors without Taskiq wrappers."""
    with pytest.raises(ClickHouseLifecycleError) as readiness_error:
        await unstarted_clickhouse_backend.is_result_ready(WRAPPER_TASK_ID)
    assert_not_ready_error(readiness_error.value)  # noqa: WPS441 - asserted pytest capture.

    with pytest.raises(ClickHouseLifecycleError) as get_error:
        await unstarted_clickhouse_backend.get_result(WRAPPER_TASK_ID)
    assert_not_ready_error(get_error.value)  # noqa: WPS441 - asserted pytest capture.


async def test_taskiq_wraps_backend_access_errors(
    unstarted_clickhouse_backend: ClickHouseResultBackend[Any],
) -> None:
    """Wrap readiness and get failures while retaining package causes."""
    task = AsyncTaskiqTask[Any](WRAPPER_TASK_ID, unstarted_clickhouse_backend)
    with pytest.raises(ResultIsReadyError) as readiness_error:
        await task.is_ready()
    assert_wrapped_not_ready(readiness_error.value)  # noqa: WPS441 - asserted pytest capture.

    with pytest.raises(ResultGetError) as get_error:
        await task.get_result()
    assert_wrapped_not_ready(get_error.value)  # noqa: WPS441 - asserted pytest capture.


async def test_wait_wraps_readiness_error(
    unstarted_clickhouse_backend: ClickHouseResultBackend[Any],
) -> None:
    """Route a wait failure through Taskiq's readiness wrapper."""
    task = AsyncTaskiqTask[Any](WRAPPER_TASK_ID, unstarted_clickhouse_backend)
    with pytest.raises(ResultIsReadyError) as wait_error:
        await task.wait_result(check_interval=0, timeout=1)
    assert_wrapped_not_ready(wait_error.value)  # noqa: WPS441 - asserted pytest capture.


async def test_taskiq_propagates_cancellation() -> None:
    """Keep cancellation outside readiness, get and wait wrapper errors."""
    task = AsyncTaskiqTask[Any](WRAPPER_TASK_ID, CancellationResultBackend())

    with pytest.raises(asyncio.CancelledError):
        await task.is_ready()
    with pytest.raises(asyncio.CancelledError):
        await task.get_result()
    with pytest.raises(asyncio.CancelledError):
        await task.wait_result(check_interval=0, timeout=1)
