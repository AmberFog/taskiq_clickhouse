"""Validated definitions shared by managed contracts and native inserts."""

from dataclasses import dataclass
from typing import Final

from taskiq_clickhouse._identifiers import Identifier, QualifiedTable
from taskiq_clickhouse._schema.contracts import ColumnContract, TableContract
from taskiq_clickhouse._schema.validation import normalize_text, require_instance, require_tuple_of


_DUPLICATE_COLUMNS: Final = "table definition columns must not contain duplicates"


@dataclass(frozen=True, slots=True)
class TableDefinition:
    """Table facts shared by schema inspection and native insert contracts.

    Static SQL resources are independently pinned against these facts by layout
    tests. Critical settings remain inspection postconditions.
    """

    columns: tuple[ColumnContract, ...]
    engine: str
    primary_key: tuple[str, ...]
    sorting_key: tuple[str, ...]
    partition_key: str = ""
    ttl_expression: str = ""
    critical_settings: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        """Reject incomplete definitions before they can form contracts."""
        columns = _validated_columns(self.columns)
        column_names = tuple(column.name.value for column in columns)
        engine = normalize_text(self.engine, field="engine", required=True)
        object.__setattr__(self, "engine", engine)
        object.__setattr__(self, "primary_key", _key(self.primary_key, field="primary key"))
        object.__setattr__(self, "sorting_key", _key(self.sorting_key, field="sorting key"))
        object.__setattr__(
            self,
            "partition_key",
            normalize_text(self.partition_key, field="partition key", required=False),
        )
        object.__setattr__(
            self,
            "ttl_expression",
            normalize_text(self.ttl_expression, field="ttl expression", required=False),
        )
        _require_declared_key(column_names, self.primary_key, field="primary key")
        _require_declared_key(column_names, self.sorting_key, field="sorting key")
        _require_primary_key_prefix(self.primary_key, self.sorting_key)

    @property
    def column_names(self) -> tuple[str, ...]:
        """Return the exact native insert order."""
        return tuple(column.name.value for column in self.columns)

    @property
    def column_types(self) -> tuple[str, ...]:
        """Return native ClickHouse type names in insert order."""
        return tuple(column.type_name for column in self.columns)

    def contract_for(self, table: QualifiedTable) -> TableContract:
        """Bind this reusable definition to one validated qualified table."""
        require_instance(table, QualifiedTable, field="table")
        return TableContract(
            table=table,
            columns=self.columns,
            engine=self.engine,
            partition_key=self.partition_key,
            sorting_key=", ".join(self.sorting_key),
            primary_key=", ".join(self.primary_key),
            ttl_expression=self.ttl_expression,
            critical_settings=self.critical_settings,
        )


def columns(*definitions: tuple[str, str]) -> tuple[ColumnContract, ...]:
    """Build immutable column contracts from compact static declarations."""
    column_contracts: list[ColumnContract] = []
    for column_name, type_name in definitions:
        column_contracts.append(ColumnContract(Identifier(column_name), type_name))
    return tuple(column_contracts)


def _key(candidate: object, *, field: str) -> tuple[str, ...]:
    key_parts = require_tuple_of(candidate, str, field=field)
    if not key_parts:
        msg = f"{field} must not be empty"
        raise ValueError(msg)
    normalized_parts = tuple(
        normalize_text(
            key_part,
            field=field,
            required=True,
        )
        for key_part in key_parts
    )
    if len(normalized_parts) != len(set(normalized_parts)):
        msg = f"{field} must not contain duplicates"
        raise ValueError(msg)
    return normalized_parts


def _validated_columns(candidate: object) -> tuple[ColumnContract, ...]:
    column_contracts = require_tuple_of(candidate, ColumnContract, field="table definition columns")
    if not column_contracts:
        msg = "table definition must declare columns"
        raise ValueError(msg)
    column_names = tuple(column.name.value for column in column_contracts)
    if len(column_names) != len(set(column_names)):
        raise ValueError(_DUPLICATE_COLUMNS)
    for column in column_contracts:
        unsupported_facts = (
            column.default_kind,
            column.default_expression,
            column.compression_codec,
            column.comment,
            column.ttl_expression,
        )
        if any(unsupported_facts):
            msg = "bootstrap column definition contains unsupported physical facts"
            raise ValueError(msg)
    return column_contracts


def _require_declared_key(
    column_names: tuple[str, ...],
    key: tuple[str, ...],
    *,
    field: str,
) -> None:
    if not set(key).issubset(column_names):
        msg = f"{field} must reference declared columns"
        raise ValueError(msg)


def _require_primary_key_prefix(
    primary_key: tuple[str, ...],
    sorting_key: tuple[str, ...],
) -> None:
    if sorting_key[: len(primary_key)] != primary_key:
        msg = "primary key must be a sorting key prefix"
        raise ValueError(msg)
