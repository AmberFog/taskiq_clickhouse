"""Validated ClickHouse identifier value objects."""

__all__ = ("Identifier", "QualifiedTable")

from dataclasses import dataclass
import re
from typing import Final


_IDENTIFIER_PATTERN: Final = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,126}\Z")
METADATA_TABLE_NAME: Final = "taskiq_clickhouse_metadata"


@dataclass(frozen=True, slots=True)
class Identifier:
    """One validated database, table or column identifier component."""

    value: str

    def __post_init__(self) -> None:
        """Reject values that cannot be quoted as one identifier component."""
        _validate_identifier(self.value)

    @property
    def quoted(self) -> str:
        """Return the safely backtick-quoted identifier."""
        return f"`{self.value}`"

    def __str__(self) -> str:
        """Return the unquoted canonical component."""
        return self.value


@dataclass(frozen=True, slots=True)
class QualifiedTable:
    """A database and table pair with unambiguous canonical forms."""

    database: Identifier
    table: Identifier

    def __post_init__(self) -> None:
        """Require already-validated identifier components."""
        _require_identifier(self.database)
        _require_identifier(self.table)

    @property
    def canonical(self) -> str:
        """Return the stable metadata representation."""
        database = self.database.value
        table = self.table.value
        return f"{database}.{table}"

    @property
    def quoted(self) -> str:
        """Return the safely quoted SQL representation."""
        database = self.database.quoted
        table = self.table.quoted
        return f"{database}.{table}"

    def __str__(self) -> str:
        """Return the canonical metadata representation."""
        return self.canonical


def _require_identifier(candidate: object) -> None:
    if not isinstance(candidate, Identifier):
        msg = "qualified table components must be Identifier instances"
        raise TypeError(msg)


def _validate_identifier(candidate: object) -> None:
    if not isinstance(candidate, str):
        msg = "identifier must match the package identifier contract"
        raise TypeError(msg)
    if _IDENTIFIER_PATTERN.fullmatch(candidate) is None:
        msg = "identifier must match the package identifier contract"
        raise ValueError(msg)
