"""Security-focused assertions shared by result-contract tests."""

import traceback

from taskiq_clickhouse.exceptions import ClickHouseResultBackendError


def assert_safe_public_error(
    error: ClickHouseResultBackendError,
    *,
    operation: str,
    reason: str,
    forbidden: str,
) -> None:
    """Require code-only diagnostics across message, repr and traceback."""
    rendered = "".join(traceback.format_exception(error))
    valid = (
        error.operation == operation,
        error.reason == reason,
        error.__cause__ is None,
        error.__context__ is None,
        forbidden not in str(error),
        forbidden not in repr(error),
        forbidden not in rendered,
    )
    if not all(valid):
        message = "public error retained unsafe state"
        raise AssertionError(message)


def assert_production_traceback_excludes(error: BaseException, *forbidden: object) -> None:
    """Require package frames to retain no caller values after a public failure."""
    production_locals: list[dict[str, object]] = []
    traceback_node = error.__traceback__
    while traceback_node is not None:
        frame = traceback_node.tb_frame
        package = frame.f_globals.get("__package__")
        if isinstance(package, str) and (package == "taskiq_clickhouse" or package.startswith("taskiq_clickhouse.")):
            production_locals.append(dict(frame.f_locals))
        traceback_node = traceback_node.tb_next
    rendered = repr(production_locals)
    if not production_locals:
        message = "public error has no package traceback frame"
        raise AssertionError(message)
    for value in forbidden:
        forbidden_text = value if type(value) is str else repr(value)  # noqa: WPS516 - exact test secrets only.
        if forbidden_text in rendered:
            message = "public error retained a forbidden traceback local"
            raise AssertionError(message)
