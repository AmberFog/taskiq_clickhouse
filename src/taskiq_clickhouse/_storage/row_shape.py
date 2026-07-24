"""Cardinality and width checks for untrusted query rows."""

from collections.abc import Sequence
from typing import TypeVar, cast


Rows = Sequence[Sequence[object]]
_NativeT = TypeVar("_NativeT")


def required_row(rows: Rows, *, width: int, projection: str) -> Sequence[object]:
    """Require one exact-width row for a mandatory projection."""
    validated = _require_rows(rows)
    if len(validated) != 1:
        msg = f"{projection} projection must return exactly one row"
        raise ValueError(msg)
    return _require_row(validated[0], width=width, projection=projection)


def optional_row(rows: Rows, *, width: int, projection: str) -> Sequence[object] | None:
    """Require zero or one exact-width row for an optional projection."""
    validated = _require_rows(rows)
    if len(validated) > 1:
        msg = f"{projection} projection must return at most one row"
        raise ValueError(msg)
    if not validated:
        return None
    return _require_row(validated[0], width=width, projection=projection)


def typed_value(candidate: object, expected_type: type[_NativeT], *, error: str) -> _NativeT:
    """Narrow one native cell before constructing a validated domain model."""
    if not isinstance(candidate, expected_type):
        raise TypeError(error)
    return candidate


def optional_typed_value(
    candidate: object,
    expected_type: type[_NativeT],
    *,
    error: str,
) -> _NativeT | None:
    """Narrow one nullable native cell before domain-model validation."""
    if candidate is None:
        return None
    return typed_value(candidate, expected_type, error=error)


def _require_rows(candidate: object) -> Sequence[Sequence[object]]:
    if not _is_row_sequence(candidate):
        msg = "query rows must be a sequence"
        raise TypeError(msg)
    return cast("Sequence[Sequence[object]]", candidate)


def _require_row(candidate: object, *, width: int, projection: str) -> Sequence[object]:
    if not _is_row_sequence(candidate):
        msg = f"{projection} row must be a sequence"
        raise TypeError(msg)
    row = cast("Sequence[object]", candidate)
    if len(row) != width:
        msg = f"{projection} row must contain exactly {width} values"
        raise ValueError(msg)
    return row


def _is_row_sequence(candidate: object) -> bool:
    if not isinstance(candidate, Sequence):
        return False
    return not isinstance(candidate, (str, bytes, bytearray))
