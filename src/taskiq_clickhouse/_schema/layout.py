"""Fixed metadata table layout and parameterized SQL."""

from dataclasses import dataclass
from types import MappingProxyType
from typing import Final

from taskiq_clickhouse._identifiers import METADATA_TABLE_NAME, Identifier, QualifiedTable
from taskiq_clickhouse._schema.contracts import SchemaContract
from taskiq_clickhouse._schema.table_definition import (
    TableDefinition,
    columns,
)
from taskiq_clickhouse._sql import bind_table, load_sql


_STRING_TYPE: Final = "String"
_BYTES_FORMAT: Final = "bytes"
MIGRATION_RECORD_KIND: Final = "migration"
MIGRATION_RECORD_KEY: Final = "schema"
NAMESPACE_RECORD_KIND: Final = "namespace"
NAMESPACE_RECORD_NAME: Final = "namespace-contract-v1"
NAMESPACE_RECORD_VERSION: Final = 1
_RECORD_KIND_COLUMN: Final = "record_kind"
_SCOPE_COLUMN: Final = "scope"
_RECORD_KEY_COLUMN: Final = "record_key"
DDL_SETTINGS: Final = MappingProxyType({"wait_end_of_query": 1})
METADATA_WRITE_SETTINGS: Final = MappingProxyType(
    {
        "async_insert": 0,
        "wait_for_async_insert": 1,
        "wait_end_of_query": 1,
    },
)
_METADATA_DEFINITION: Final = TableDefinition(
    columns=columns(
        (_RECORD_KIND_COLUMN, _STRING_TYPE),
        (_SCOPE_COLUMN, _STRING_TYPE),
        (_RECORD_KEY_COLUMN, _STRING_TYPE),
        ("version", "UInt32"),
        ("name", _STRING_TYPE),
        ("payload", _STRING_TYPE),
        ("checksum", _STRING_TYPE),
        ("package_version", _STRING_TYPE),
        ("recorded_at", "DateTime64(6, 'UTC')"),
        ("attempt_id", "UUID"),
    ),
    engine="MergeTree",
    primary_key=(_RECORD_KIND_COLUMN, _SCOPE_COLUMN, _RECORD_KEY_COLUMN, "version"),
    sorting_key=(
        _RECORD_KIND_COLUMN,
        _SCOPE_COLUMN,
        _RECORD_KEY_COLUMN,
        "version",
        "checksum",
        "recorded_at",
        "attempt_id",
    ),
)
METADATA_COLUMN_NAMES: Final = _METADATA_DEFINITION.column_names
METADATA_COLUMN_TYPES: Final = _METADATA_DEFINITION.column_types
STRING_COLUMN_FORMATS: Final = MappingProxyType(
    {
        _RECORD_KIND_COLUMN: _BYTES_FORMAT,
        _SCOPE_COLUMN: _BYTES_FORMAT,
        _RECORD_KEY_COLUMN: _BYTES_FORMAT,
        "name": _BYTES_FORMAT,
        "payload": _BYTES_FORMAT,
        "checksum": _BYTES_FORMAT,
        "package_version": _BYTES_FORMAT,
    },
)
SERVER_NOW_QUERY: Final = load_sql("schema/server_now.sql")
_CREATE_QUERY: Final = load_sql("schema/create_metadata_table.sql")
_READ_QUERY: Final = load_sql("schema/metadata_read.sql")
_CONFIRMATION_QUERY: Final = load_sql("schema/metadata_confirmation.sql")


@dataclass(frozen=True, slots=True)
class MetadataLayout:
    """Resolve the fixed metadata contract inside one validated database."""

    database: Identifier

    @property
    def table(self) -> QualifiedTable:
        """Return the fixed qualified metadata table."""
        return QualifiedTable(database=self.database, table=Identifier(METADATA_TABLE_NAME))

    @property
    def contract(self) -> SchemaContract:
        """Return the exact physical postcondition."""
        return SchemaContract(tables=(_METADATA_DEFINITION.contract_for(self.table),))

    @property
    def create_query(self) -> str:
        """Return the fixed parameterized bootstrap DDL."""
        return _CREATE_QUERY

    @property
    def read_query(self) -> str:
        """Return the ordered complete-record read query."""
        return _READ_QUERY

    @property
    def confirmation_query(self) -> str:
        """Return the complete-row confirmation query for a frozen identity."""
        return _CONFIRMATION_QUERY

    @property
    def table_parameters(self) -> dict[str, object]:
        """Bind the fixed metadata table without SQL interpolation."""
        return bind_table(self.table)
