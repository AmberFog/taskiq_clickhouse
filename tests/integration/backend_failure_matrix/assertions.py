"""Safe-error and traceback assertions for real driver failures."""

from __future__ import annotations

import logging
import traceback
from typing import TYPE_CHECKING, Final


if TYPE_CHECKING:
    from taskiq_clickhouse.exceptions import ClickHouseResultBackendError


_LOGGER: Final = logging.getLogger("tests.integration.backend_failure_matrix")
_LOG_MESSAGE: Final = "captured sanitized ClickHouse backend failure"


def log_public_error(error: ClickHouseResultBackendError) -> None:
    """Render the public traceback through ordinary application logging."""
    exception_info = (type(error), error, error.__traceback__)
    _LOGGER.error(_LOG_MESSAGE, exc_info=exception_info)


def assert_safe_public_error(
    error: ClickHouseResultBackendError,
    *,
    operation: str,
    reason: str,
    forbidden: tuple[str, ...],
    log_text: str,
) -> None:
    """Require code-only diagnostics without raw credentials or payloads."""
    rendered_traceback = "".join(traceback.format_exception(error))
    rendered = "\n".join((str(error), repr(error), rendered_traceback, log_text))
    _require(condition=error.operation == operation, message="unexpected safe operation code")
    _require(condition=error.reason == reason, message="unexpected safe reason code")
    _require(condition=error.__cause__ is None, message="public error retained a cause")
    _require(condition=error.__context__ is None, message="public error retained a context")
    _require(condition=str(error) in log_text, message="captured log omitted the public failure")
    for private_value in forbidden:
        _require(
            condition=private_value not in rendered,
            message=f"private value leaked: {private_value!r}",
        )


def _require(*, condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)
