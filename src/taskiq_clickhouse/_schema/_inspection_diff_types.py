"""Immutable differences produced by physical-schema comparison."""

from dataclasses import dataclass

from taskiq_clickhouse._identifiers import QualifiedTable


@dataclass(frozen=True, slots=True)
class SchemaMismatch:
    """One safe, structured physical-schema difference."""

    table: QualifiedTable
    path: str
    expected: object
    actual: object


@dataclass(frozen=True, slots=True)
class SchemaDifference:
    """All differences found for one expected schema phase."""

    mismatches: tuple[SchemaMismatch, ...]

    @property
    def matches(self) -> bool:
        """Return whether the physical schema matches the complete contract."""
        return not self.mismatches
