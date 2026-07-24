"""Unit assertions for Taskiq public-boundary scenarios."""

# ruff: noqa: S101

from logging import LogRecord

from taskiq_clickhouse.exceptions import ClickHouseLifecycleError
from tests.taskiq_boundary import constants as boundary_constants
from tests.taskiq_boundary.models import ReceiverFailureCase


def assert_not_ready_error(error: ClickHouseLifecycleError) -> None:
    """Assert the stable direct lifecycle failure."""
    assert error.operation == "backend"
    assert error.reason == "not_ready"


def assert_wrapped_not_ready(error: BaseException) -> None:
    """Assert a Taskiq wrapper retains the stable package cause."""
    cause = error.__cause__
    assert isinstance(cause, ClickHouseLifecycleError)
    assert cause.operation == "backend"
    assert cause.reason == "not_ready"


def assert_receiver_default(case: ReceiverFailureCase) -> None:
    """Assert default worker handling executes and acknowledges."""
    assert case.events == ["task", "ack"]


def assert_receiver_log_is_safe(records: list[LogRecord], text: str) -> None:
    """Assert the receiver logs one detached package failure only."""
    error_records = [
        record
        for record in records
        if record.name == boundary_constants.TASKIQ_RECEIVER_LOGGER
        and record.levelno == boundary_constants.RECEIVER_ERROR_LEVEL
    ]
    assert len(error_records) == 1
    assert boundary_constants.SAFE_NOT_READY_MESSAGE in error_records[0].getMessage()
    assert error_records[0].exc_info is not None
    assert isinstance(error_records[0].exc_info[1], ClickHouseLifecycleError)
    for private_value in boundary_constants.REDACTED_RECEIVER_VALUES:
        assert private_value not in text
