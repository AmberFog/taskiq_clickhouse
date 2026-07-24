"""Neutral value-only diagnostics for physical-schema drift."""

from dataclasses import dataclass
import re
from typing import Final

from taskiq_clickhouse._identifiers import QualifiedTable


_SAFE_PATH_PATTERN: Final = re.compile(r"[A-Za-z][A-Za-z0-9_.\[\]]{0,255}\Z")


@dataclass(frozen=True, slots=True)
class SchemaDriftLocation:
    """One validated table/path coordinate without physical values."""

    table: QualifiedTable
    path: str

    def __post_init__(self) -> None:
        """Reject objects or paths that are not package-safe coordinates."""
        if type(self.table) is not QualifiedTable:  # noqa: WPS516 - public diagnostics reject value-object subclasses.
            msg = "drift location table must be a QualifiedTable"
            raise TypeError(msg)
        if type(self.path) is not str:  # noqa: WPS516 - arbitrary string subclasses may expose hooks.
            msg = "drift location path must be a string"
            raise TypeError(msg)
        if _SAFE_PATH_PATTERN.fullmatch(self.path) is None:
            msg = "drift location path must be a safe coordinate"
            raise ValueError(msg)


@dataclass(frozen=True, slots=True)
class SchemaDriftReport:
    """Complete immutable drift coordinates detached from catalog values."""

    mismatch_count: int
    locations: tuple[SchemaDriftLocation, ...]

    def __post_init__(self) -> None:
        """Require one exact location for every counted mismatch."""
        _require_mismatch_count(self.mismatch_count)
        _require_locations(self.locations, mismatch_count=self.mismatch_count)


def _require_mismatch_count(candidate: object) -> None:
    if type(candidate) is not int:  # noqa: WPS516 - booleans and integer subclasses are invalid diagnostics.
        msg = "drift mismatch count must be an integer"
        raise TypeError(msg)
    if candidate < 1:
        msg = "drift mismatch count must be positive"
        raise ValueError(msg)


def _require_locations(candidate: object, *, mismatch_count: int) -> None:
    if type(candidate) is not tuple:  # noqa: WPS516 - diagnostics are exact immutable DTOs.
        msg = "drift locations must be an exact location tuple"
        raise TypeError(msg)
    exact_locations = all(
        type(location) is SchemaDriftLocation  # noqa: WPS516 - subclasses may retain arbitrary state.
        for location in candidate
    )
    if not exact_locations:
        msg = "drift locations must contain exact location values"
        raise TypeError(msg)
    if len(candidate) != mismatch_count:
        msg = "drift locations must match the mismatch count"
        raise ValueError(msg)
