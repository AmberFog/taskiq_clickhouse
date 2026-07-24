"""Test-only Taskiq result models and exception shapes."""

from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from taskiq.result import TaskiqResult


class PublicContractError(Exception):
    """Application exception that must retain its concrete imported type."""


def unique_namespace(prefix: str) -> str:
    """Return one collision-resistant namespace accepted by the public grammar."""
    return f"{prefix}-{uuid4().hex}"


@dataclass(frozen=True, slots=True)
class SuccessCase:
    """Complete successful Taskiq result and its expected immutable values."""

    marker: str
    log: str | None
    execution_time: float = 1.0

    def build(self) -> TaskiqResult[Any]:
        """Build one result with distinct values for every mapped field."""
        return TaskiqResult[Any](
            is_err=False,
            log=self.log,
            return_value={
                "marker": self.marker,
                "nested": [1, True, None, {"leaf": "original"}],
            },
            execution_time=self.execution_time,
            labels={"case": self.marker, "queue": "integration"},
            error=None,
        )


def builtin_error_result() -> TaskiqResult[Any]:
    """Build a built-in exception with an explicit cause."""
    task_error = ValueError("builtin-error")
    task_error.__cause__ = RuntimeError("builtin-cause")
    return TaskiqResult[Any](
        is_err=True,
        log="builtin-error-log",
        return_value=None,
        execution_time=1.0,
        labels={"kind": "builtin", "sequence": 1},
        error=task_error,
    )


def custom_error_result() -> TaskiqResult[Any]:
    """Build an imported application exception with explicit context."""
    task_error = PublicContractError("custom-error")
    task_error.__context__ = LookupError("custom-context")
    task_error.__suppress_context__ = False
    return TaskiqResult[Any](
        is_err=True,
        log="custom-error-log",
        return_value=None,
        execution_time=1.0,
        labels={"kind": "custom", "sequence": 2},
        error=task_error,
    )
