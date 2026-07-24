"""Validated immutable native insert request."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import Self, TypeGuard, cast


_FrozenRows = tuple[tuple[object, ...], ...]


@dataclass(frozen=True, slots=True)
class _InsertShape:
    names: tuple[str, ...]
    types: tuple[str, ...]

    @classmethod
    def copy(cls, names: object, types: object) -> Self:
        copied = cls(
            _copy_text_sequence(names, field="column_names"),
            _copy_text_sequence(types, field="column_type_names"),
        )
        if len(copied.names) != len(copied.types):
            msg = "column_names and column_type_names must have equal length"
            raise ValueError(msg)
        return copied

    def copy_rows(self, candidate: object) -> _FrozenRows:
        """Copy a non-empty batch matching the declared physical width."""
        if not _is_value_sequence(candidate):
            msg = "rows must be a sequence"
            raise TypeError(msg)
        rows = tuple(self._copy_row(candidate_row) for candidate_row in candidate)
        if not rows:
            msg = "rows must not be empty"
            raise ValueError(msg)
        return rows

    def _copy_row(self, candidate: object) -> tuple[object, ...]:
        if not _is_value_sequence(candidate):
            msg = "each row must be a sequence"
            raise TypeError(msg)
        row = tuple(candidate)
        if len(row) != len(self.names):
            msg = "each row must match the declared column count"
            raise ValueError(msg)
        return row


@dataclass(frozen=True, slots=True, repr=False)
class InsertRequest:
    """One immutable native insert without an implicit schema lookup."""

    database: str
    table: str
    rows: Sequence[Sequence[object]]
    column_names: Sequence[str]
    column_type_names: Sequence[str]
    settings: Mapping[str, object]

    def __post_init__(self) -> None:
        """Copy mutable inputs and reject incoherent row shapes."""
        shape = _InsertShape.copy(self.column_names, self.column_type_names)
        rows = shape.copy_rows(self.rows)
        object.__setattr__(self, "database", _require_non_empty_text(self.database, field="database"))
        object.__setattr__(self, "table", _require_non_empty_text(self.table, field="table"))
        object.__setattr__(self, "rows", rows)
        object.__setattr__(self, "column_names", shape.names)
        object.__setattr__(self, "column_type_names", shape.types)
        object.__setattr__(self, "settings", _copy_settings(self.settings))


def _copy_text_sequence(candidate: object, *, field: str) -> tuple[str, ...]:
    if not _is_value_sequence(candidate):
        msg = f"{field} must be a sequence"
        raise TypeError(msg)
    copied: tuple[object, ...] = tuple(candidate)
    if not copied:
        msg = f"{field} must not be empty"
        raise ValueError(msg)
    for candidate_value in copied:
        exact_string = type(candidate_value) is str  # noqa: WPS516 - exact physical names only.
        if not exact_string or not candidate_value:
            msg = f"{field} must contain non-empty strings"
            raise TypeError(msg)
    return cast("tuple[str, ...]", copied)


def _copy_settings(candidate: object) -> Mapping[str, object]:
    if not isinstance(candidate, Mapping):
        msg = "settings must be a mapping"
        raise TypeError(msg)
    copied = dict(candidate)
    if not all(type(key) is str for key in copied):  # noqa: WPS516 - driver setting names are exact strings.
        msg = "settings keys must be strings"
        raise TypeError(msg)
    return MappingProxyType(cast("dict[str, object]", copied))


def _require_non_empty_text(candidate: object, *, field: str) -> str:
    if type(candidate) is not str:  # noqa: WPS516 - identifiers reject coercible subclasses.
        msg = f"{field} must be a string"
        raise TypeError(msg)
    if not candidate:
        msg = f"{field} must not be empty"
        raise ValueError(msg)
    return candidate


def _is_value_sequence(candidate: object) -> TypeGuard[Sequence[object]]:
    if not isinstance(candidate, Sequence):
        return False
    return not isinstance(candidate, (str, bytes, bytearray))
