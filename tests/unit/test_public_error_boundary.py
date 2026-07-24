"""Verify identity and diagnostic policy of exported exception decorators."""

import pytest

from taskiq_clickhouse._public_error_boundary import (
    detach_public_errors,
    detach_public_errors_async,
)
from tests.result_contract.assertions import assert_production_traceback_excludes


_PRIVATE_ARGUMENT = "PRIVATE_PUBLIC_ARGUMENT"
_PRIVATE_CONTEXT = "PRIVATE_TERMINAL_CONTEXT"


class _TerminalSignal(BaseException):
    """Represent one synchronous process-level public signal."""


def test_sync_terminal_signal_is_identity_preserving_and_detached() -> None:
    """Clear terminal chains and wrapper arguments without replacing identity."""
    terminal = _TerminalSignal("terminal")
    terminal.__cause__ = RuntimeError(_PRIVATE_CONTEXT)
    terminal.__context__ = RuntimeError(_PRIVATE_CONTEXT)

    @detach_public_errors
    def fail(private_argument: str) -> None:
        del private_argument
        raise terminal

    with pytest.raises(_TerminalSignal) as raised:
        fail(_PRIVATE_ARGUMENT)

    assert raised.value is terminal
    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None
    assert raised.value.__suppress_context__
    assert_production_traceback_excludes(
        raised.value,
        _PRIVATE_ARGUMENT,
        _PRIVATE_CONTEXT,
    )


@pytest.mark.asyncio
async def test_async_ordinary_error_preserves_programmer_diagnostics() -> None:
    """Leave a raw ordinary error and its implementation traceback untouched."""
    ordinary_error = RuntimeError("programmer-error")

    @detach_public_errors_async
    async def fail() -> None:
        raise ordinary_error

    with pytest.raises(RuntimeError) as raised:
        await fail()

    assert raised.value is ordinary_error
    traceback_names: list[str] = []
    traceback_node = raised.value.__traceback__
    while traceback_node is not None:
        traceback_names.append(traceback_node.tb_frame.f_code.co_name)
        traceback_node = traceback_node.tb_next
    assert "fail" in traceback_names
