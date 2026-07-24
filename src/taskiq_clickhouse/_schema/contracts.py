"""Immutable physical table and schema contracts."""

from dataclasses import dataclass
from operator import attrgetter
import re
from typing import Final, cast

from taskiq_clickhouse._identifiers import Identifier, QualifiedTable
from taskiq_clickhouse._schema.validation import normalize_text, require_instance, require_tuple_of


_SETTING_NAME_PATTERN: Final = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_SETTING_PAIR_LENGTH: Final = 2
_TTL_FIELD: Final = "ttl_expression"
_ADDITIVE_FIELD: Final = "allowed_additive_columns"
_AUXILIARY_OBJECTS_FIELD: Final = "auxiliary_objects"
_INVALID_SETTING_NAME: Final = "critical setting name is invalid"


@dataclass(frozen=True, slots=True)
class ColumnContract:
    """Exact physical contract for one ClickHouse column."""

    name: Identifier
    type_name: str
    default_kind: str = ""
    default_expression: str = ""
    compression_codec: str = ""
    comment: str = ""
    ttl_expression: str = ""

    def __post_init__(self) -> None:
        """Validate catalog values without changing expression contents."""
        require_instance(self.name, Identifier, field="column name")
        object.__setattr__(
            self,
            "type_name",
            normalize_text(self.type_name, field="column type", required=True),
        )
        for field_name in (
            "default_kind",
            "default_expression",
            "compression_codec",
            "comment",
            _TTL_FIELD,
        ):
            catalog_value = cast("str", getattr(self, field_name))
            normalized = normalize_text(catalog_value, field=field_name, required=False)
            object.__setattr__(self, field_name, normalized)
        if self.ttl_expression:
            msg = "managed columns must not declare column TTL"
            raise ValueError(msg)

    def canonical_data(self) -> dict[str, object]:
        """Return normalized column data for checksum encoding."""
        return {
            "comment": self.comment,
            "compression_codec": self.compression_codec,
            "default_expression": self.default_expression,
            "default_kind": self.default_kind,
            "name": self.name.value,
            _TTL_FIELD: self.ttl_expression,
            "type": self.type_name,
        }


@dataclass(frozen=True, slots=True)
class TableContract:
    """Exact normalized physical contract for one managed v0.1 table.

    Constraints, data-skipping indices, dependent materialized views and
    projections are deliberately absent from the v0.1 feature surface. Their
    required absence is part of the canonical contract rather than an implicit
    inspection policy.
    """

    table: QualifiedTable
    columns: tuple[ColumnContract, ...]
    engine: str
    partition_key: str
    sorting_key: str
    primary_key: str
    sampling_key: str = ""
    ttl_expression: str = ""
    critical_settings: tuple[tuple[str, str], ...] = ()
    allowed_additive_columns: tuple[ColumnContract, ...] = ()

    def __post_init__(self) -> None:
        """Validate columns, keys and the compatibility allowlist."""
        require_instance(self.table, QualifiedTable, field="table")
        require_tuple_of(self.columns, ColumnContract, field="columns")
        if not self.columns:
            msg = "table contract must declare columns"
            raise ValueError(msg)
        _unique_columns(self.columns, field="columns")
        self._normalize_table_expressions()
        object.__setattr__(self, "critical_settings", _settings(self.critical_settings))
        self._validate_additive_columns()

    def canonical_data(self) -> dict[str, object]:
        """Return normalized table data for checksum encoding."""
        return {
            _ADDITIVE_FIELD: [column.canonical_data() for column in self.allowed_additive_columns],
            _AUXILIARY_OBJECTS_FIELD: {
                "constraints": [],
                "data_skipping_indices": [],
                "materialized_views": [],
                "projections": [],
            },
            "columns": [column.canonical_data() for column in self.columns],
            "critical_settings": [[name, setting_value] for name, setting_value in self.critical_settings],
            "engine": self.engine,
            "partition_key": self.partition_key,
            "primary_key": self.primary_key,
            "sampling_key": self.sampling_key,
            "sorting_key": self.sorting_key,
            "table": self.table.canonical,
            _TTL_FIELD: self.ttl_expression,
        }

    def _normalize_table_expressions(self) -> None:
        engine = normalize_text(self.engine, field="engine", required=True)
        object.__setattr__(self, "engine", engine)
        for field_name, required in (
            ("partition_key", False),
            ("sorting_key", True),
            ("primary_key", True),
            ("sampling_key", False),
            (_TTL_FIELD, False),
        ):
            expression = cast("str", getattr(self, field_name))
            normalized = normalize_text(expression, field=field_name, required=required)
            object.__setattr__(self, field_name, normalized)

    def _validate_additive_columns(self) -> None:
        require_tuple_of(
            self.allowed_additive_columns,
            ColumnContract,
            field=_ADDITIVE_FIELD,
        )
        _unique_columns(self.allowed_additive_columns, field=_ADDITIVE_FIELD)
        base_names = {column.name.value for column in self.columns}
        for column in self.allowed_additive_columns:
            if column.name.value in base_names:
                msg = "additive column duplicates a required column"
                raise ValueError(msg)
            if column.default_kind != "DEFAULT" or not column.default_expression:
                msg = "allowed additive columns require an explicit DEFAULT"
                raise ValueError(msg)
        additions = sorted(
            self.allowed_additive_columns,
            key=attrgetter("name.value"),
        )
        object.__setattr__(self, _ADDITIVE_FIELD, tuple(additions))


@dataclass(frozen=True, slots=True)
class SchemaContract:
    """Complete expected present and absent table state after one step."""

    tables: tuple[TableContract, ...] = ()
    absent_tables: tuple[QualifiedTable, ...] = ()

    def __post_init__(self) -> None:
        """Canonicalize table order and reject contradictory expectations."""
        require_tuple_of(self.tables, TableContract, field="tables")
        require_tuple_of(self.absent_tables, QualifiedTable, field="absent_tables")
        tables = tuple(sorted(self.tables, key=attrgetter("table.canonical")))
        absent_tables = tuple(sorted(self.absent_tables, key=attrgetter("canonical")))
        present_names = [contract.table.canonical for contract in tables]
        absent_names = [table.canonical for table in absent_tables]
        _unique(present_names, field="schema tables")
        _unique(absent_names, field="absent schema tables")
        if set(present_names) & set(absent_names):
            msg = "schema cannot require the same table to be present and absent"
            raise ValueError(msg)
        object.__setattr__(self, "tables", tables)
        object.__setattr__(self, "absent_tables", absent_tables)

    def canonical_data(self) -> dict[str, object]:
        """Return normalized schema data for checksum encoding."""
        return {
            "absent_tables": [table.canonical for table in self.absent_tables],
            "tables": [table.canonical_data() for table in self.tables],
        }


def _unique(candidates: list[str], *, field: str) -> None:
    if len(candidates) != len(set(candidates)):
        msg = f"{field} must not contain duplicates"
        raise ValueError(msg)


def _unique_columns(columns: tuple[ColumnContract, ...], *, field: str) -> None:
    _unique([column.name.value for column in columns], field=field)


def _settings(settings: object) -> tuple[tuple[str, str], ...]:
    if not isinstance(settings, tuple):
        msg = "critical_settings must be a tuple"
        raise TypeError(msg)
    normalized = [_setting(setting) for setting in settings]
    normalized.sort(key=lambda setting_pair: setting_pair[0])
    _unique([name for name, _setting_value in normalized], field="critical settings")
    return tuple(normalized)


def _setting(setting: object) -> tuple[str, str]:
    if not isinstance(setting, tuple) or len(setting) != _SETTING_PAIR_LENGTH:
        msg = "each critical setting must be a name/value tuple"
        raise TypeError(msg)
    name, setting_value = setting
    if not isinstance(name, str):
        raise TypeError(_INVALID_SETTING_NAME)
    if _SETTING_NAME_PATTERN.fullmatch(name) is None:
        raise ValueError(_INVALID_SETTING_NAME)
    normalized_value = normalize_text(setting_value, field="critical setting value", required=True)
    return name, normalized_value
