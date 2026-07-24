"""Detach sensitive implementation frames at exported public call boundaries."""

from collections.abc import Callable, Coroutine
from functools import wraps
from typing import Any, ParamSpec, TypeVar

from taskiq_clickhouse.exceptions import ClickHouseResultBackendError, rebuild_public_error


_CallSpec = ParamSpec("_CallSpec")
_ResultT = TypeVar("_ResultT")


def _detach_terminal(error: BaseException) -> BaseException:
    """Clear retained implementation state without replacing a terminal signal."""
    error.__traceback__ = None
    error.__cause__ = None
    error.__context__ = None
    error.__suppress_context__ = True
    return error


def detach_public_errors(
    function: Callable[_CallSpec, _ResultT],
) -> Callable[_CallSpec, _ResultT]:
    """Detach package errors and terminal signals after releasing sync arguments."""

    @wraps(function)
    def protected(*args: _CallSpec.args, **kwargs: _CallSpec.kwargs) -> _ResultT:
        error_to_raise: BaseException
        try:
            return function(*args, **kwargs)
        except ClickHouseResultBackendError as error:
            error_to_raise = rebuild_public_error(error)
        except BaseException as error:  # Only terminal signals are detached below.
            if isinstance(error, Exception):
                raise
            error_to_raise = _detach_terminal(error)
        del args, kwargs
        raise error_to_raise from None

    return protected


def detach_public_errors_async(
    function: Callable[_CallSpec, Coroutine[Any, Any, _ResultT]],
) -> Callable[_CallSpec, Coroutine[Any, Any, _ResultT]]:
    """Detach package errors and terminal signals after releasing async arguments."""

    @wraps(function)
    async def protected(*args: _CallSpec.args, **kwargs: _CallSpec.kwargs) -> _ResultT:
        error_to_raise: BaseException
        try:
            return await function(*args, **kwargs)
        except ClickHouseResultBackendError as error:
            error_to_raise = rebuild_public_error(error)
        except BaseException as error:  # Only terminal signals are detached below.
            if isinstance(error, Exception):
                raise
            error_to_raise = _detach_terminal(error)
        del args, kwargs
        raise error_to_raise from None

    return protected
