"""Immutable values produced by physical-schema inspection."""

from dataclasses import dataclass

from taskiq_clickhouse._identifiers import QualifiedTable


class SchemaInspectionError(RuntimeError):
    """Report a malformed or internally inconsistent catalog response."""


@dataclass(frozen=True, slots=True)
class AuxiliaryObjectsSnapshot:
    """Presence flags for v0.1-forbidden table-level objects."""

    constraints: bool
    data_skipping_indices: bool
    materialized_views: bool
    projections: bool


@dataclass(frozen=True, slots=True)
class SystemColumnSnapshot:
    """One normalized row from ``system.columns``."""

    position: int
    name: str
    type_name: str
    default_kind: str
    default_expression: str
    compression_codec: str


@dataclass(frozen=True, slots=True)
class DescribedColumnSnapshot:
    """One normalized row from ``DESCRIBE TABLE``."""

    name: str
    type_name: str
    default_kind: str
    default_expression: str
    comment: str
    compression_codec: str
    ttl_expression: str


@dataclass(frozen=True, slots=True)
class TableSnapshot:
    """Normalized physical facts for one existing table."""

    table: QualifiedTable
    engine: str
    partition_key: str
    sorting_key: str
    primary_key: str
    sampling_key: str
    ttl_expression: str
    settings: tuple[tuple[str, str], ...]
    auxiliary_objects: AuxiliaryObjectsSnapshot
    columns: tuple[SystemColumnSnapshot, ...]
    described_columns: tuple[DescribedColumnSnapshot, ...]


@dataclass(frozen=True, slots=True)
class SchemaSnapshot:
    """Physical observations for every table named by a schema contract."""

    tables: tuple[TableSnapshot, ...]
    absent_tables: tuple[QualifiedTable, ...]

    def table(self, qualified_table: QualifiedTable) -> TableSnapshot | None:
        """Return one observed table, distinguishing absence from no observation."""
        for snapshot in self.tables:
            if snapshot.table == qualified_table:
                return snapshot
        if qualified_table in self.absent_tables:
            return None
        msg = f"table was not inspected: {qualified_table.canonical}"
        raise SchemaInspectionError(msg)
