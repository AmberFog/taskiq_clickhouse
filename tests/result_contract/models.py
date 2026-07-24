"""Stable result values used by public and serializer contract tests."""

from datetime import UTC, datetime
from typing import Any

from taskiq.result import TaskiqResult


class ContractTaskError(RuntimeError):
    """Application error whose module-level identity can be reconstructed."""


class PythonOnlyValue:
    """A non-JSON value used to prove opt-in Pickle behavior."""

    __slots__ = ("created_at", "name")

    def __init__(self, name: str, created_at: datetime) -> None:
        """Retain an immutable-by-convention custom Python value."""
        self.name = name
        self.created_at = created_at

    def __eq__(self, candidate: object) -> bool:
        """Compare exact custom values for the lossless round trip."""
        if not isinstance(candidate, PythonOnlyValue):
            return NotImplemented
        return self.name == candidate.name and self.created_at == candidate.created_at

    def __hash__(self) -> int:
        """Hash the same immutable value pair used by equality."""
        return hash((self.name, self.created_at))


def success_result(
    *,
    return_value: object | None = None,
    log: str | None = "contract-log",
) -> TaskiqResult[Any]:
    """Build one complete successful Taskiq result."""
    payload = {"nested": [1, True, None, "value"]} if return_value is None else return_value
    return TaskiqResult[Any](
        is_err=False,
        log=log,
        return_value=payload,
        execution_time=0.375,
        labels={"queue": "contract", "attempt": 2},
        error=None,
    )


def error_result(index: int = 0) -> TaskiqResult[Any]:
    """Build one custom application error with a built-in explicit cause."""
    cause = ValueError(f"cause-{index}")
    error = ContractTaskError(f"failure-{index}")
    error.__cause__ = cause
    return TaskiqResult[Any](
        is_err=True,
        log="error-log",
        return_value=None,
        execution_time=0.625,
        labels={"queue": "contract", "index": index},
        error=error,
    )


def context_error_result() -> TaskiqResult[Any]:
    """Build an application error with an implicit exception context."""
    captured = ContractTaskError("context-owner")
    captured.__context__ = LookupError("context-value")
    return TaskiqResult[Any](
        is_err=True,
        log=None,
        return_value=None,
        execution_time=0.75,
        labels={"queue": "contract"},
        error=captured,
    )


def python_only_graph() -> dict[str, object]:
    """Return a lossless Pickle graph deliberately rejected by JSON."""
    return {
        "bytes": b"\x00\xffpayload",
        "members": {"alpha", "beta"},
        "coordinates": (1, 2),
        "created_at": datetime(2026, 7, 16, 12, 30, tzinfo=UTC),
        "custom": PythonOnlyValue(
            name="custom",
            created_at=datetime(2026, 7, 16, 12, 31, tzinfo=UTC),
        ),
    }
