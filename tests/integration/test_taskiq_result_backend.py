"""Exercise the ClickHouse backend through real Taskiq public boundaries."""

from typing import Any

import pytest
from taskiq import AsyncTaskiqTask, ResultGetError

from taskiq_clickhouse.exceptions import ClickHouseResultNotFoundError
from tests.integration.settings import ClickHouseTestSettings
from tests.taskiq_boundary.constants import MISSING_TASK_ID, SUCCESS_VALUE
from tests.taskiq_boundary.integration_actions import (
    observe_task_error,
    observe_typed_success,
    taskiq_receiver_harness,
)
from tests.taskiq_boundary.result_assertions import (
    assert_custom_task_error,
    assert_missing_errors,
    assert_receiver_metadata,
    assert_task_error_chain,
    assert_task_error_raises,
    assert_typed_fetch,
    assert_typed_success,
)


pytestmark = [pytest.mark.clickhouse, pytest.mark.asyncio]


async def test_receiver_returns_typed_result(
    clickhouse_settings: ClickHouseTestSettings,
    clickhouse_database: str,
) -> None:
    """Run a gated receiver task and reconstruct its annotated return type."""
    async with taskiq_receiver_harness(clickhouse_settings, clickhouse_database) as harness:
        observation = await observe_typed_success(harness, SUCCESS_VALUE)

        assert_typed_success(observation)
        assert_receiver_metadata(observation.waited)
        assert_typed_fetch(observation)


async def test_receiver_returns_custom_task_error(
    clickhouse_settings: ClickHouseTestSettings,
    clickhouse_database: str,
) -> None:
    """Return a task failure as data with its exact custom type and cause."""
    async with taskiq_receiver_harness(clickhouse_settings, clickhouse_database) as harness:
        task_result = await observe_task_error(harness)

        assert_receiver_metadata(task_result)
        assert_custom_task_error(task_result)
        assert_task_error_chain(task_result)
        assert_task_error_raises(task_result)


async def test_missing_result_direct_and_wrapped(
    clickhouse_settings: ClickHouseTestSettings,
    clickhouse_database: str,
) -> None:
    """Keep absence observable while wrapping only the Taskiq get boundary."""
    async with taskiq_receiver_harness(clickhouse_settings, clickhouse_database) as harness:
        assert not await harness.backend.is_result_ready(MISSING_TASK_ID)
        with pytest.raises(ClickHouseResultNotFoundError) as direct_error:
            await harness.backend.get_result(MISSING_TASK_ID, with_logs=True)

        task = AsyncTaskiqTask[Any](MISSING_TASK_ID, harness.backend)
        assert not await task.is_ready()
        with pytest.raises(ResultGetError) as wrapped_error:
            await task.get_result(with_logs=True)

        assert_missing_errors(
            direct_error.value,  # noqa: WPS441 - asserted pytest capture.
            wrapped_error.value,  # noqa: WPS441 - asserted pytest capture.
        )
