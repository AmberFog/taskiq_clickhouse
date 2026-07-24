"""Shared strict runtime validation for immutable schema models."""

from typing import Final, TypeVar, cast


_UINT32_MAX: Final = 4_294_967_295
_ItemT = TypeVar("_ItemT")


def require_instance(candidate: object, item_type: type[object], *, field: str) -> None:
    """Require one value of the expected runtime type."""
    if not isinstance(candidate, item_type):
        msg = f"{field} must be a {item_type.__name__}"
        raise TypeError(msg)


def require_tuple_of(
    candidate: object,
    item_type: type[_ItemT],
    *,
    field: str,
) -> tuple[_ItemT, ...]:
    """Require one immutable homogeneous tuple."""
    if not isinstance(candidate, tuple):
        msg = f"{field} must be a tuple of {item_type.__name__}"
        raise TypeError(msg)
    if not all(isinstance(element, item_type) for element in candidate):
        msg = f"{field} must be a tuple of {item_type.__name__}"
        raise TypeError(msg)
    return cast("tuple[_ItemT, ...]", candidate)


def require_uint32(number: int, *, field: str) -> None:
    """Require a positive non-boolean UInt32 integer."""
    if not isinstance(number, int) or isinstance(number, bool):
        msg = f"{field} must be an integer"
        raise TypeError(msg)
    if not 1 <= number <= _UINT32_MAX:
        msg = f"{field} must fit UInt32 and be positive"
        raise ValueError(msg)


def require_bool(candidate: object, *, field: str) -> None:
    """Require a real boolean."""
    if not isinstance(candidate, bool):
        msg = f"{field} must be a boolean"
        raise TypeError(msg)


def normalize_text(candidate: object, *, field: str, required: bool) -> str:
    """Normalize contract text while preserving internal whitespace."""
    if not isinstance(candidate, str):
        msg = f"{field} must be a string"
        raise TypeError(msg)
    if "\x00" in candidate:
        msg = f"{field} must not contain NUL"
        raise ValueError(msg)
    normalized_newlines = candidate.replace("\r\n", "\n").replace("\r", "\n")
    normalized = normalized_newlines.strip()
    if required and not normalized:
        msg = f"{field} must not be empty"
        raise ValueError(msg)
    return normalized


def require_nonempty_text(candidate: object, *, field: str) -> None:
    """Require nonempty NUL-free metadata text."""
    normalize_text(candidate, field=field, required=True)
