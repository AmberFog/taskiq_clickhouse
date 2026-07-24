"""Normalized physical-schema inspection for managed ClickHouse tables."""

from collections.abc import Sequence
from types import MappingProxyType
from typing import Final

from taskiq_clickhouse._clickhouse import (
    contracts as clickhouse_contracts,
    queries as clickhouse_queries,
)
from taskiq_clickhouse._identifiers import QualifiedTable
from taskiq_clickhouse._schema import (
    _drift_projection as drift_projection,
    _inspection_diff as inspection_diff,
    _inspection_diff_types as inspection_diff_types,
    _inspection_types as inspection_types,
)
from taskiq_clickhouse._schema._inspection_sql import (
    CATALOG_VALUE_PARSER,
    SQL_EXPRESSION_PARSER,
)
from taskiq_clickhouse._schema.contracts import SchemaContract
from taskiq_clickhouse._schema_drift import SchemaDriftReport
from taskiq_clickhouse._sql import bind_table, load_sql
from taskiq_clickhouse.exceptions import _PhysicalSchemaDriftError


SYSTEM_TABLE_QUERY: Final = load_sql("schema/inspect_table.sql")
SYSTEM_COLUMNS_QUERY: Final = load_sql("schema/inspect_columns.sql")
DESCRIBE_TABLE_QUERY: Final = load_sql("schema/describe_table.sql")

_BYTES_FORMAT: Final = "bytes"
_TABLE_STRING_COLUMNS: Final = MappingProxyType(
    {
        "engine": _BYTES_FORMAT,
        "engine_full": _BYTES_FORMAT,
        "partition_key": _BYTES_FORMAT,
        "sorting_key": _BYTES_FORMAT,
        "primary_key": _BYTES_FORMAT,
        "sampling_key": _BYTES_FORMAT,
        "create_table_query": _BYTES_FORMAT,
        "formatted_create_table_query": _BYTES_FORMAT,
        "formatted_constraint_probe": _BYTES_FORMAT,
    }
)
_SYSTEM_COLUMN_STRING_COLUMNS: Final = MappingProxyType(
    {
        "name": _BYTES_FORMAT,
        "type": _BYTES_FORMAT,
        "default_kind": _BYTES_FORMAT,
        "default_expression": _BYTES_FORMAT,
        "compression_codec": _BYTES_FORMAT,
    }
)
_DESCRIBE_STRING_COLUMNS: Final = MappingProxyType(
    {
        "name": _BYTES_FORMAT,
        "type": _BYTES_FORMAT,
        "default_type": _BYTES_FORMAT,
        "default_expression": _BYTES_FORMAT,
        "comment": _BYTES_FORMAT,
        "codec_expression": _BYTES_FORMAT,
        "ttl_expression": _BYTES_FORMAT,
    }
)
_TABLE_ROW_SIZE: Final = 12
_SYSTEM_COLUMN_ROW_SIZE: Final = 6
_DESCRIBE_ROW_SIZE: Final = 7
_FORMATTED_CREATE_QUERY_INDEX: Final = 7
_FORMATTED_CONSTRAINT_PROBE_INDEX: Final = 8
_DATA_SKIPPING_INDEX_FLAG_INDEX: Final = 9
_PROJECTION_FLAG_INDEX: Final = 10
_MATERIALIZED_VIEW_FLAG_INDEX: Final = 11


class SchemaInspector:
    """Read and validate normalized physical ClickHouse schema facts."""

    __slots__ = ("gateway",)

    def __init__(self, gateway: clickhouse_contracts.RowsReader) -> None:
        self.gateway = gateway

    async def inspect(self, contract: SchemaContract) -> inspection_types.SchemaSnapshot:
        """Capture a structured snapshot for a before/after schema contract."""
        present_tables = tuple(table_contract.table for table_contract in contract.tables)
        qualified_tables = present_tables + contract.absent_tables
        snapshots: list[inspection_types.TableSnapshot | None] = []
        for table in qualified_tables:
            snapshot = await self._inspect_table(table)  # noqa: WPS476  # Avoid orphan catalog queries on failure.
            snapshots.append(snapshot)
        return _schema_snapshot(qualified_tables, snapshots)

    async def diff(self, contract: SchemaContract) -> inspection_diff_types.SchemaDifference:
        """Return every mismatch without converting it into an exception."""
        return inspection_diff.compare_schema(await self.inspect(contract), contract)

    async def matches(self, contract: SchemaContract) -> bool:
        """Return false for an absent, unexpected or physically drifted schema."""
        return (await self.diff(contract)).matches

    async def validate(self, contract: SchemaContract) -> None:
        """Raise one typed error containing only safe drift coordinates."""
        report = await self._safe_drift_report(contract)
        if report is not None:
            raise _PhysicalSchemaDriftError(report)

    async def _safe_drift_report(
        self,
        contract: SchemaContract,
    ) -> SchemaDriftReport | None:
        """Discard raw catalog values before a public traceback is created."""
        difference = await self.diff(contract)
        if difference.matches:
            return None
        return drift_projection.safe_drift_report(difference)

    async def _inspect_table(
        self,
        table: QualifiedTable,
    ) -> inspection_types.TableSnapshot | None:
        query_parameters = bind_table(table)
        table_rows = await clickhouse_queries.query_rows(
            self.gateway,
            clickhouse_queries.QueryRequest(
                query=SYSTEM_TABLE_QUERY,
                operation="schema_inspection",
                query_parameters=query_parameters,
                column_formats=_TABLE_STRING_COLUMNS,
            ),
        )
        if not table_rows:
            return None
        if len(table_rows) != 1:
            msg = f"system.tables returned multiple rows for {table.canonical}"
            raise inspection_types.SchemaInspectionError(msg)
        column_rows = await clickhouse_queries.query_rows(
            self.gateway,
            clickhouse_queries.QueryRequest(
                query=SYSTEM_COLUMNS_QUERY,
                operation="schema_inspection",
                query_parameters=query_parameters,
                column_formats=_SYSTEM_COLUMN_STRING_COLUMNS,
            ),
        )
        describe_rows = await clickhouse_queries.query_rows(
            self.gateway,
            clickhouse_queries.QueryRequest(
                query=DESCRIBE_TABLE_QUERY,
                operation="schema_inspection",
                query_parameters=query_parameters,
                column_formats=_DESCRIBE_STRING_COLUMNS,
            ),
        )
        return _table_snapshot(table, table_rows[0], column_rows, describe_rows)


def _schema_snapshot(
    qualified_tables: tuple[QualifiedTable, ...],
    snapshots: Sequence[inspection_types.TableSnapshot | None],
) -> inspection_types.SchemaSnapshot:
    tables = tuple(snapshot for snapshot in snapshots if snapshot is not None)
    absent_tables: list[QualifiedTable] = []
    for table, snapshot in zip(qualified_tables, snapshots, strict=True):
        if snapshot is None:
            absent_tables.append(table)
    return inspection_types.SchemaSnapshot(
        tables=tables,
        absent_tables=tuple(absent_tables),
    )


def _table_snapshot(
    table: QualifiedTable,
    table_row: Sequence[object],
    column_rows: Sequence[Sequence[object]],
    describe_rows: Sequence[Sequence[object]],
) -> inspection_types.TableSnapshot:
    CATALOG_VALUE_PARSER.require_row_size(table_row, _TABLE_ROW_SIZE, "system.tables")
    engine_full = CATALOG_VALUE_PARSER.text(table_row[1], "system.tables.engine_full")
    create_table_query = CATALOG_VALUE_PARSER.text(table_row[6], "system.tables.create_table_query")
    return inspection_types.TableSnapshot(
        table=table,
        engine=SQL_EXPRESSION_PARSER.normalize(CATALOG_VALUE_PARSER.text(table_row[0], "system.tables.engine")),
        partition_key=SQL_EXPRESSION_PARSER.normalize(
            CATALOG_VALUE_PARSER.text(table_row[2], "system.tables.partition_key")
        ),
        sorting_key=SQL_EXPRESSION_PARSER.normalize(
            CATALOG_VALUE_PARSER.text(table_row[3], "system.tables.sorting_key")
        ),
        primary_key=SQL_EXPRESSION_PARSER.normalize(
            CATALOG_VALUE_PARSER.text(table_row[4], "system.tables.primary_key")
        ),
        sampling_key=SQL_EXPRESSION_PARSER.normalize(
            CATALOG_VALUE_PARSER.text(table_row[5], "system.tables.sampling_key")
        ),
        ttl_expression=SQL_EXPRESSION_PARSER.table_ttl(create_table_query),
        settings=SQL_EXPRESSION_PARSER.engine_settings(engine_full),
        auxiliary_objects=_auxiliary_objects_snapshot(table_row),
        columns=tuple(_system_column(row) for row in column_rows),
        described_columns=tuple(_described_column(row) for row in describe_rows),
    )


def _auxiliary_objects_snapshot(
    table_row: Sequence[object],
) -> inspection_types.AuxiliaryObjectsSnapshot:
    formatted_create_query = CATALOG_VALUE_PARSER.text(
        table_row[_FORMATTED_CREATE_QUERY_INDEX],
        "system.tables.formatted_create_table_query",
    )
    formatted_constraint_probe = CATALOG_VALUE_PARSER.text(
        table_row[_FORMATTED_CONSTRAINT_PROBE_INDEX],
        "system.tables.formatted_constraint_probe",
    )
    if not SQL_EXPRESSION_PARSER.has_constraints(formatted_constraint_probe):
        msg = "formatQuery constraint capability is unsupported"
        raise inspection_types.SchemaInspectionError(msg)
    return inspection_types.AuxiliaryObjectsSnapshot(
        constraints=SQL_EXPRESSION_PARSER.has_constraints(formatted_create_query),
        data_skipping_indices=CATALOG_VALUE_PARSER.binary_flag(
            table_row[_DATA_SKIPPING_INDEX_FLAG_INDEX],
            "system.data_skipping_indices.exists",
        ),
        materialized_views=CATALOG_VALUE_PARSER.binary_flag(
            table_row[_MATERIALIZED_VIEW_FLAG_INDEX],
            "system.tables.dependent_materialized_views",
        ),
        projections=CATALOG_VALUE_PARSER.binary_flag(
            table_row[_PROJECTION_FLAG_INDEX],
            "system.projections.exists",
        ),
    )


def _system_column(row: Sequence[object]) -> inspection_types.SystemColumnSnapshot:
    CATALOG_VALUE_PARSER.require_row_size(row, _SYSTEM_COLUMN_ROW_SIZE, "system.columns")
    return inspection_types.SystemColumnSnapshot(
        position=CATALOG_VALUE_PARSER.positive_int(row[0], "system.columns.position"),
        name=CATALOG_VALUE_PARSER.text(row[1], "system.columns.name"),
        type_name=SQL_EXPRESSION_PARSER.normalize(CATALOG_VALUE_PARSER.text(row[2], "system.columns.type")),
        default_kind=SQL_EXPRESSION_PARSER.normalize(CATALOG_VALUE_PARSER.text(row[3], "system.columns.default_kind")),
        default_expression=SQL_EXPRESSION_PARSER.normalize(
            CATALOG_VALUE_PARSER.text(row[4], "system.columns.default_expression")
        ),
        compression_codec=SQL_EXPRESSION_PARSER.normalize(
            CATALOG_VALUE_PARSER.text(row[5], "system.columns.compression_codec")
        ),
    )


def _described_column(row: Sequence[object]) -> inspection_types.DescribedColumnSnapshot:
    CATALOG_VALUE_PARSER.require_row_size(row, _DESCRIBE_ROW_SIZE, "DESCRIBE TABLE")
    return inspection_types.DescribedColumnSnapshot(
        name=CATALOG_VALUE_PARSER.text(row[0], "describe.name"),
        type_name=SQL_EXPRESSION_PARSER.normalize(CATALOG_VALUE_PARSER.text(row[1], "describe.type")),
        default_kind=SQL_EXPRESSION_PARSER.normalize(CATALOG_VALUE_PARSER.text(row[2], "describe.default_type")),
        default_expression=SQL_EXPRESSION_PARSER.normalize(
            CATALOG_VALUE_PARSER.text(row[3], "describe.default_expression")
        ),
        comment=CATALOG_VALUE_PARSER.text(row[4], "describe.comment"),
        compression_codec=SQL_EXPRESSION_PARSER.normalize(
            CATALOG_VALUE_PARSER.text(row[5], "describe.codec_expression")
        ),
        ttl_expression=SQL_EXPRESSION_PARSER.normalize(CATALOG_VALUE_PARSER.text(row[6], "describe.ttl_expression")),
    )


def compare_schema(
    snapshot: inspection_types.SchemaSnapshot,
    contract: SchemaContract,
) -> inspection_diff_types.SchemaDifference:
    """Compare one captured snapshot with an exact expected schema phase."""
    return inspection_diff.compare_schema(snapshot, contract)
