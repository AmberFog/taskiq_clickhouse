"""Integration assertions for Taskiq result-boundary scenarios."""

# ruff: noqa: S101

from typing import Any

import pytest
from taskiq import ResultGetError
from taskiq.result import TaskiqResult

from taskiq_clickhouse.exceptions import ClickHouseResultNotFoundError
from tests.taskiq_boundary import constants as boundary_constants
from tests.taskiq_boundary.models import (
    ReceiverTaskError,
    TypedReceiverResult,
    TypedSuccessObservation,
)


def assert_receiver_metadata(task_result: TaskiqResult[Any]) -> None:
    """Assert fields populated by the real Taskiq receiver."""
    assert task_result.labels == boundary_constants.EXPECTED_TASK_LABELS
    assert task_result.execution_time.__class__ is float
    assert task_result.execution_time >= 0
    assert task_result.log is None


def assert_typed_success(observation: TypedSuccessObservation) -> None:
    """Assert readiness and annotated return reconstruction."""
    waited = observation.waited
    assert not observation.ready_before_completion
    assert waited.return_value.__class__ is TypedReceiverResult
    assert waited.return_value == TypedReceiverResult(answer=boundary_constants.SUCCESS_VALUE)
    assert not waited.is_err
    assert waited.error is None


def assert_typed_fetch(observation: TypedSuccessObservation) -> None:
    """Assert a subsequent with-log get remains typed and log-free."""
    assert observation.fetched.return_value.__class__ is TypedReceiverResult
    assert observation.fetched.return_value == observation.waited.return_value
    assert observation.fetched.log is None


def assert_custom_task_error(task_result: TaskiqResult[None]) -> None:
    """Assert custom failure identity and top-level result fields."""
    error = task_result.error
    assert task_result.is_err
    assert task_result.return_value is None
    assert error is not None
    assert error.__class__ is ReceiverTaskError
    assert str(error) == boundary_constants.TASK_ERROR_MESSAGE


def assert_task_error_chain(task_result: TaskiqResult[None]) -> None:
    """Assert custom cause and context reconstruction."""
    error = task_result.error
    assert error is not None
    cause = error.__cause__
    assert error.__context__ is None
    if cause is None:
        pytest.fail("task error cause is missing")
    assert cause.__class__ is ValueError
    assert str(cause) == boundary_constants.TASK_ERROR_CAUSE
    assert error.__suppress_context__


def assert_task_error_raises(task_result: TaskiqResult[None]) -> None:
    """Assert explicit user-side raising preserves error identity."""
    error = task_result.error
    assert error is not None
    with pytest.raises(ReceiverTaskError) as raised:
        task_result.raise_for_error()
    assert raised.value is error  # noqa: WPS441 - asserted pytest capture.


def assert_missing_errors(
    direct_error: ClickHouseResultNotFoundError,
    wrapped_error: ResultGetError,
) -> None:
    """Assert direct absence and its Taskiq get wrapper."""
    assert direct_error.operation == "result_read"
    assert direct_error.reason == "not_found"
    cause = wrapped_error.__cause__
    assert isinstance(cause, ClickHouseResultNotFoundError)
    assert cause.operation == "result_read"
    assert cause.reason == "not_found"
