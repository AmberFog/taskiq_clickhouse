"""Verify receiver acknowledgement and sanitized save-failure behavior."""

import logging

import pytest

from taskiq_clickhouse.exceptions import ClickHouseLifecycleError
from tests.taskiq_boundary.assertions import (
    assert_not_ready_error,
    assert_receiver_default,
    assert_receiver_log_is_safe,
)
from tests.taskiq_boundary.constants import TASKIQ_RECEIVER_LOGGER
from tests.taskiq_boundary.models import ReceiverFailureCase
from tests.taskiq_boundary.taskiq_actions import observe_direct_set_failure


pytestmark = pytest.mark.asyncio


async def test_receiver_logs_safe_failure_and_acks(
    receiver_failure_case: ReceiverFailureCase,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Record Taskiq's default worker behavior without leaking private data."""
    with pytest.raises(ClickHouseLifecycleError) as direct_failure:
        await observe_direct_set_failure(receiver_failure_case.backend)
    assert_not_ready_error(direct_failure.value)  # noqa: WPS441 - asserted pytest capture.

    caplog.clear()
    with caplog.at_level(logging.ERROR, logger=TASKIQ_RECEIVER_LOGGER):
        await receiver_failure_case.receiver.callback(receiver_failure_case.message)

    assert_receiver_default(receiver_failure_case)
    assert_receiver_log_is_safe(caplog.records, caplog.text)


async def test_receiver_raise_err_prevents_ack(
    receiver_failure_case: ReceiverFailureCase,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Expose the diagnostic callback mode separately from worker behavior."""
    with (
        caplog.at_level(logging.ERROR, logger=TASKIQ_RECEIVER_LOGGER),
        pytest.raises(ClickHouseLifecycleError) as raised,
    ):
        await receiver_failure_case.receiver.callback(
            receiver_failure_case.message,
            raise_err=True,
        )

    assert_not_ready_error(raised.value)  # noqa: WPS441 - asserted pytest capture.
    assert receiver_failure_case.events == ["task"]
