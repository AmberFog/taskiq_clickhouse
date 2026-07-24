"""Immutable production storage layout and migration v1."""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final, cast

from taskiq_clickhouse._identifiers import Identifier, QualifiedTable
from taskiq_clickhouse._schema.contracts import ColumnContract, SchemaContract, TableContract
from taskiq_clickhouse._schema.migrations import (
    MigrationDefinition,
    MigrationStep,
    SchemaPlan,
)
from taskiq_clickhouse._schema.table_definition import (
    TableDefinition,
    columns,
)
from taskiq_clickhouse._schema.validation import require_instance
from taskiq_clickhouse._sql import bind_table, load_sql
from taskiq_clickhouse._types import MigrationExecution


MIGRATION_V1_NAME: Final = "create_result_and_progress_tables"
_STRING_TYPE: Final = "String"
_DATETIME_TYPE: Final = "DateTime64(6, 'UTC')"
_COMMON_COLUMNS: Final = (
    ("namespace", _STRING_TYPE),
    ("task_id", _STRING_TYPE),
    ("generation_at", _DATETIME_TYPE),
    ("generation_id", "UUID"),
)
_RETENTION_COLUMNS: Final = (
    ("written_at", _DATETIME_TYPE),
    ("visible_until", _DATETIME_TYPE),
    ("purge_at", _DATETIME_TYPE),
)
_PRIMARY_KEY: Final = ("namespace", "task_id")
_RESULT_SORTING_KEY: Final = (*_PRIMARY_KEY, "generation_at", "generation_id", "state")
_PROGRESS_SORTING_KEY: Final = (*_PRIMARY_KEY, "generation_at", "generation_id")


def _storage_definition(
    table_columns: tuple[ColumnContract, ...],
    *,
    sorting_key: tuple[str, ...],
) -> TableDefinition:
    return TableDefinition(
        columns=table_columns,
        engine="MergeTree",
        partition_key="toYYYYMM(purge_at)",
        primary_key=_PRIMARY_KEY,
        sorting_key=sorting_key,
        ttl_expression="purge_at",
    )


_RESULT_DEFINITION: Final = _storage_definition(
    columns(
        *_COMMON_COLUMNS,
        ("state", "UInt8"),
        *_RETENTION_COLUMNS,
        ("result_payload", _STRING_TYPE),
        ("log_payload", _STRING_TYPE),
    ),
    sorting_key=_RESULT_SORTING_KEY,
)
_PROGRESS_DEFINITION: Final = _storage_definition(
    columns(
        *_COMMON_COLUMNS,
        *_RETENTION_COLUMNS,
        ("progress_payload", _STRING_TYPE),
    ),
    sorting_key=_PROGRESS_SORTING_KEY,
)
RESULT_COLUMN_NAMES: Final = _RESULT_DEFINITION.column_names
RESULT_COLUMN_TYPES: Final = _RESULT_DEFINITION.column_types
PROGRESS_COLUMN_NAMES: Final = _PROGRESS_DEFINITION.column_names
PROGRESS_COLUMN_TYPES: Final = _PROGRESS_DEFINITION.column_types
_CREATE_RESULT_QUERY: Final = load_sql("migrations/v001_create_result_table.sql")
_CREATE_PROGRESS_QUERY: Final = load_sql("migrations/v001_create_progress_table.sql")


@dataclass(frozen=True, slots=True)
class StorageLayout:  # noqa: WPS214 - cohesive immutable schema/query resource view.
    """Resolve result and progress storage inside one validated database."""

    result_table: QualifiedTable
    progress_table: QualifiedTable

    def __post_init__(self) -> None:
        """Require distinct qualified tables in the same database."""
        require_instance(self.result_table, QualifiedTable, field="result table")
        require_instance(self.progress_table, QualifiedTable, field="progress table")
        if self.result_table == self.progress_table:
            msg = "result and progress tables must differ"
            raise ValueError(msg)
        if self.result_table.database != self.progress_table.database:
            msg = "result and progress tables must share one database"
            raise ValueError(msg)

    @property
    def database(self) -> Identifier:
        """Return the shared validated database identifier."""
        return self.result_table.database

    @property
    def result_contract(self) -> TableContract:
        """Return the exact result-table physical contract."""
        return _RESULT_DEFINITION.contract_for(self.result_table)

    @property
    def progress_contract(self) -> TableContract:
        """Return the exact progress-table physical contract."""
        return _PROGRESS_DEFINITION.contract_for(self.progress_table)

    @property
    def target_contract(self) -> SchemaContract:
        """Return the complete physical storage postcondition."""
        return SchemaContract(tables=(self.result_contract, self.progress_contract))

    @property
    def create_result_query(self) -> str:
        """Return the retry-safe result-table DDL."""
        return _CREATE_RESULT_QUERY

    @property
    def create_progress_query(self) -> str:
        """Return the retry-safe progress-table DDL."""
        return _CREATE_PROGRESS_QUERY

    @property
    def create_result_parameters(self) -> Mapping[str, str]:
        """Bind the result-table identifiers for its fixed DDL."""
        return cast("Mapping[str, str]", bind_table(self.result_table))

    @property
    def create_progress_parameters(self) -> Mapping[str, str]:
        """Bind the progress-table identifiers for its fixed DDL."""
        return cast("Mapping[str, str]", bind_table(self.progress_table))


def build_storage_plan(layout: StorageLayout) -> SchemaPlan:
    """Build deterministic migration v1 for one validated storage layout."""
    require_instance(layout, StorageLayout, field="storage layout")
    result_contract = layout.result_contract
    progress_contract = layout.progress_contract
    empty_storage = SchemaContract(
        absent_tables=(layout.result_table, layout.progress_table),
    )
    result_ready = SchemaContract(tables=(result_contract,))
    storage_ready = SchemaContract(tables=(result_contract, progress_contract))
    return SchemaPlan(
        (
            MigrationDefinition(
                version=1,
                name=MIGRATION_V1_NAME,
                execution=MigrationExecution.AUTO,
                reentrant=True,
                concurrent_safe=True,
                steps=(
                    MigrationStep(
                        ddl=layout.create_result_query,
                        before=empty_storage,
                        after=result_ready,
                        query_parameters=layout.create_result_parameters,
                    ),
                    MigrationStep(
                        ddl=layout.create_progress_query,
                        before=result_ready,
                        after=storage_ready,
                        query_parameters=layout.create_progress_parameters,
                    ),
                ),
            ),
        ),
    )


def storage_layout_from_names(
    database: str,
    result_table: str,
    progress_table: str,
) -> StorageLayout:
    """Build one layout while validating every identifier component."""
    database_identifier = Identifier(database)
    return StorageLayout(
        result_table=QualifiedTable(database_identifier, Identifier(result_table)),
        progress_table=QualifiedTable(database_identifier, Identifier(progress_table)),
    )
