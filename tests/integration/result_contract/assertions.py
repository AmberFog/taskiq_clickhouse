"""Domain assertions for complete public Taskiq results."""

# ruff: noqa: S101

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from typing import Any

    from taskiq.result import TaskiqResult


@dataclass(frozen=True, slots=True)
class ErrorExpectation:
    """Expected concrete error and one reconstructed chain edge."""

    error_type: type[BaseException]
    chain_attribute: str
    chain_type: type[BaseException]
    chain_message: str


def assert_success_result(
    observed: TaskiqResult[Any],
    expected: TaskiqResult[Any],
    *,
    with_logs: bool,
) -> None:
    """Assert every stable successful result field without hiding log policy."""
    expected_log = expected.log if with_logs else None
    _assert_common_fields(observed, expected)
    assert observed.is_err is False
    assert observed.error is None
    assert observed.log == expected_log


def assert_error_result(
    observed: TaskiqResult[Any],
    expected: TaskiqResult[Any],
    expectation: ErrorExpectation,
) -> None:
    """Assert result fields and one reconstructed exception-chain edge."""
    _assert_common_fields(observed, expected)
    _assert_error_shape(observed, expected, expectation)


def _assert_common_fields(
    observed: TaskiqResult[Any],
    expected: TaskiqResult[Any],
) -> None:
    assert observed.return_value == expected.return_value
    assert observed.execution_time == expected.execution_time
    assert observed.labels == expected.labels


def _assert_error_shape(
    observed: TaskiqResult[Any],
    expected: TaskiqResult[Any],
    expectation: ErrorExpectation,
) -> None:
    assert observed.is_err is True
    assert observed.log == expected.log
    assert type(observed.error) is expectation.error_type  # noqa: WPS516 - exact error class is contractual.
    assert observed.error is not None
    assert observed.error.args == expected.error.args  # type: ignore[union-attr]
    _assert_error_chain(observed.error, expectation)


def _assert_error_chain(
    task_error: BaseException,
    expectation: ErrorExpectation,
) -> None:
    chained = getattr(task_error, expectation.chain_attribute)
    assert type(chained) is expectation.chain_type  # noqa: WPS516 - exact chain class is contractual.
    assert str(chained) == expectation.chain_message
