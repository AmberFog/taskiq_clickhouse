"""Render bounded, secret-free schema CLI failure reports."""

import re
from typing import Final, TypeGuard, TypeVar

from taskiq_clickhouse._cli_drift_reporting import render_drift_lines
from taskiq_clickhouse.exceptions import ClickHouseResultBackendError


_PROGRAM: Final = "taskiq-clickhouse-schema"
_GENERIC_FAILURE: Final = f"{_PROGRAM}: schema operation failed"
_SAFE_CODE_PATTERN: Final = re.compile(r"[a-z][a-z0-9_]{0,63}\Z")
_ExactT = TypeVar("_ExactT")


def render_operation_failure(error: Exception) -> str:
    """Return one stable report without evaluating raw exception text."""
    try:
        return _render_operation_failure(error)
    except Exception:  # noqa: BLE001 - reporting must fail closed without replacing the primary failure.
        return f"{_GENERIC_FAILURE}\n"


def _render_operation_failure(error: Exception) -> str:
    public_codes = _public_codes(error)
    if public_codes is None:
        return f"{_GENERIC_FAILURE}\n"
    operation, reason = public_codes
    header = f"{_GENERIC_FAILURE} [{operation}:{reason}]"
    body = "\n".join((header, *render_drift_lines(error)))
    return f"{body}\n"


def _public_codes(error: Exception) -> tuple[str, str] | None:
    if not isinstance(error, ClickHouseResultBackendError):
        return None
    operation = _safe_code(error.operation)
    reason = _safe_code(error.reason)
    if operation is not None and reason is not None:
        return operation, reason
    return None


def _safe_code(candidate: object) -> str | None:
    if _has_exact_type(candidate, str) and _SAFE_CODE_PATTERN.fullmatch(candidate) is not None:
        return candidate
    return None


def _has_exact_type(candidate: object, expected: type[_ExactT]) -> TypeGuard[_ExactT]:
    return type(candidate) is expected  # noqa: WPS516 - reject attacker-controlled diagnostic subclasses.
