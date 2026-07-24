"""Unit tests for normalized physical ClickHouse schema inspection."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from typing import Final

import pytest

from taskiq_clickhouse._clickhouse.queries import UNCACHED_READ_SETTINGS
from taskiq_clickhouse._identifiers import Identifier, QualifiedTable
from taskiq_clickhouse._schema._inspection_sql import SQL_EXPRESSION_PARSER
from taskiq_clickhouse._schema._inspection_types import (
    SchemaInspectionError,
    SchemaSnapshot,
)
from taskiq_clickhouse._schema.contracts import ColumnContract, SchemaContract
from taskiq_clickhouse._schema.inspection import (
    DESCRIBE_TABLE_QUERY,
    SYSTEM_COLUMNS_QUERY,
    SYSTEM_TABLE_QUERY,
    SchemaInspector,
    compare_schema,
)
from taskiq_clickhouse._schema.layout import MetadataLayout
from taskiq_clickhouse._schema_drift import (
    SchemaDriftLocation,
    SchemaDriftReport,
)
from taskiq_clickhouse.exceptions import _PhysicalSchemaDriftError


DATABASE: Final = Identifier("inspection_test")
METADATA_LAYOUT: Final = MetadataLayout(DATABASE)
METADATA_TABLE: Final = METADATA_LAYOUT.table
METADATA_CONTRACT: Final = METADATA_LAYOUT.contract.tables[0]
COMPATIBLE_ADDITIVE: Final = ColumnContract(
    name=Identifier("metadata_format"),
    type_name="UInt8",
    default_kind="DEFAULT",
    default_expression="1",
)
DEFAULT_ENGINE_FULL: Final = (
    "MergeTree PRIMARY KEY (record_kind, scope, record_key, version) "
    "ORDER BY (record_kind, scope, record_key, version, checksum, recorded_at, attempt_id) "
    "SETTINGS index_granularity = 8192"
)
DEFAULT_CREATE_QUERY: Final = (
    "CREATE TABLE inspection_test.taskiq_clickhouse_metadata "
    "(`record_kind` String) ENGINE = MergeTree "
    "PRIMARY KEY (record_kind, scope, record_key, version) "
    "ORDER BY (record_kind, scope, record_key, version, checksum, recorded_at, attempt_id) "
    "SETTINGS index_granularity = 8192"
)
DEFAULT_FORMATTED_CREATE_QUERY: Final = """CREATE TABLE inspection_test.taskiq_clickhouse_metadata
(
    `record_kind` String
)
ENGINE = MergeTree
PRIMARY KEY (record_kind, scope, record_key, version)
ORDER BY (record_kind, scope, record_key, version, checksum, recorded_at, attempt_id)
SETTINGS index_granularity = 8192"""
DEFAULT_FORMATTED_CONSTRAINT_PROBE: Final = """CREATE TABLE taskiq_clickhouse_constraint_probe
(
    `value` UInt8,
    CONSTRAINT taskiq_probe CHECK value > 0
)
ENGINE = MergeTree
ORDER BY value"""
SECRET_CATALOG_VALUE: Final = "password=raw-default dsn=https://private.internal"  # noqa: S105  # pragma: allowlist secret


@dataclass(frozen=True, slots=True)
class _PhysicalRows:
    table: tuple[tuple[object, ...], ...]
    columns: tuple[tuple[object, ...], ...]
    describe: tuple[tuple[object, ...], ...]


@dataclass(frozen=True, slots=True)
class _QueryCall:
    query: str
    parameters: Mapping[str, object] | None
    settings: Mapping[str, object] | None
    column_formats: Mapping[str, str] | None


class _FakeGateway:
    def __init__(self, rows: _PhysicalRows) -> None:
        self._rows = rows
        self.calls: list[_QueryCall] = []

    async def query_rows(
        self,
        query: str,
        *,
        query_parameters: Mapping[str, object] | None = None,
        settings: Mapping[str, object] | None = None,
        column_formats: Mapping[str, str] | None = None,
    ) -> tuple[tuple[object, ...], ...]:
        assert settings is UNCACHED_READ_SETTINGS
        self.calls.append(_QueryCall(query, query_parameters, settings, column_formats))
        if "FROM system.tables" in query:
            return self._rows.table
        if "FROM system.columns" in query:
            return self._rows.columns
        if query.lstrip().startswith("DESCRIBE TABLE"):
            return self._rows.describe
        msg = "unexpected query"
        raise AssertionError(msg)


@pytest.mark.asyncio
async def test_exact_metadata_contract_uses_normalized_parameterized_reads() -> None:
    """Accept the exact fixed schema while ignoring an unowned server default."""
    contract = METADATA_CONTRACT
    gateway = _FakeGateway(_exact_rows(contract.columns))
    inspector = SchemaInspector(gateway)

    snapshot = await inspector.inspect(SchemaContract(tables=(contract,)))
    other_table = QualifiedTable(DATABASE, Identifier("other_table"))
    mixed_snapshot = SchemaSnapshot(tables=snapshot.tables, absent_tables=(other_table,))

    assert snapshot.table(METADATA_TABLE) is not None
    assert mixed_snapshot.table(other_table) is None
    assert await inspector.matches(SchemaContract(tables=(contract,)))
    assert (await inspector.diff(SchemaContract(tables=(contract,)))).matches
    await inspector.validate(SchemaContract(tables=(contract,)))
    assert len(gateway.calls) == 4 * 3
    assert gateway.calls[0].query == SYSTEM_TABLE_QUERY
    assert gateway.calls[0].parameters == {
        "database": DATABASE.value,
        "table": METADATA_TABLE.table.value,
    }
    assert set(gateway.calls[0].column_formats or {}) == {
        "engine",
        "engine_full",
        "partition_key",
        "sorting_key",
        "primary_key",
        "sampling_key",
        "create_table_query",
        "formatted_create_table_query",
        "formatted_constraint_probe",
    }
    assert "FROM system.data_skipping_indices" in gateway.calls[0].query
    assert "FROM system.projections" in gateway.calls[0].query
    assert "notEmpty(dependencies_database)" in gateway.calls[0].query
    assert "notEmpty(dependencies_table)" in gateway.calls[0].query
    assert "formatQuery(create_table_query)" in gateway.calls[0].query
    assert gateway.calls[1].query == SYSTEM_COLUMNS_QUERY
    assert gateway.calls[2].query == DESCRIBE_TABLE_QUERY
    assert gateway.calls[2].parameters == gateway.calls[0].parameters
    assert all(call.settings is UNCACHED_READ_SETTINGS for call in gateway.calls)
    assert all(value == "bytes" for call in gateway.calls for value in (call.column_formats or {}).values())


@pytest.mark.asyncio
async def test_absent_and_present_contracts_are_distinct_phases() -> None:
    """Represent structured before/after states without interpreting absence as I/O failure."""
    table_contract = METADATA_CONTRACT
    before = SchemaContract(absent_tables=(METADATA_TABLE,))
    after = SchemaContract(tables=(table_contract,))
    absent_inspector = SchemaInspector(_FakeGateway(_PhysicalRows((), (), ())))

    before_snapshot = await absent_inspector.inspect(before)

    assert await absent_inspector.matches(before)
    assert not await absent_inspector.matches(after)
    assert before_snapshot.table(METADATA_TABLE) is None
    with pytest.raises(SchemaInspectionError, match="was not inspected"):
        before_snapshot.table(QualifiedTable(DATABASE, Identifier("unknown_table")))
    with pytest.raises(_PhysicalSchemaDriftError) as captured:
        await absent_inspector.validate(after)
    assert captured.value.operation == "schema_validation"
    assert captured.value.reason == "physical_drift"
    assert captured.value.report == SchemaDriftReport(
        mismatch_count=1,
        locations=(SchemaDriftLocation(METADATA_TABLE, "table"),),
    )
    assert str(captured.value) == "ClickHouse operation failed [schema_validation:physical_drift]"
    assert METADATA_TABLE.canonical not in repr(captured.value)
    assert "present" not in repr(captured.value)

    present_snapshot = await SchemaInspector(_FakeGateway(_exact_rows(table_contract.columns))).inspect(before)
    difference = compare_schema(present_snapshot, before)
    assert not difference.matches
    assert difference.mismatches[0].expected == "absent"
    assert difference.mismatches[0].actual == "present"


@pytest.mark.asyncio
async def test_drift_reports_owned_facts_but_not_unowned_settings() -> None:
    """Report exact schema drift while leaving server-default settings outside the contract."""
    table_contract = replace(
        METADATA_CONTRACT,
        critical_settings=(("allow_nullable_key", "1"),),
    )
    rows = _exact_rows(table_contract.columns)
    first_column = list(rows.columns[0])
    first_column[2] = b"LowCardinality(String)"
    first_description = list(rows.describe[0])
    first_description[2] = b"DEFAULT"
    first_description[3] = b"'a  b'"
    drifted_table = list(rows.table[0])
    drifted_table[0] = b"ReplacingMergeTree"
    drifted_table[6] = (
        b"CREATE TABLE inspection_test.taskiq_clickhouse_metadata (`record_kind` String) "
        b"ENGINE = ReplacingMergeTree TTL recorded_at DELETE SETTINGS index_granularity = 8192"
    )
    drifted_rows = _PhysicalRows(
        table=(tuple(drifted_table),),
        columns=(tuple(first_column), *rows.columns[1:]),
        describe=(tuple(first_description), *rows.describe[1:]),
    )

    difference = await SchemaInspector(_FakeGateway(drifted_rows)).diff(SchemaContract(tables=(table_contract,)))
    paths = {mismatch.path for mismatch in difference.mismatches}

    assert "engine" in paths
    assert "ttl_expression" in paths
    assert "settings.allow_nullable_key" in paths
    assert "columns[0].type" in paths
    assert "columns[0].describe.default_kind" in paths
    assert "columns[0].describe.default_expression" in paths
    assert "settings.index_granularity" not in paths


@pytest.mark.asyncio
async def test_validate_detaches_raw_catalog_values_from_public_drift() -> None:
    """Retain count and locations without retaining raw DEFAULT/comment text."""
    rows = _exact_rows(METADATA_CONTRACT.columns)
    first_system = list(rows.columns[0])
    first_system[4] = SECRET_CATALOG_VALUE.encode()
    first_description = list(rows.describe[0])
    first_description[3] = SECRET_CATALOG_VALUE.encode()
    first_description[4] = SECRET_CATALOG_VALUE.encode()
    drifted = replace(
        rows,
        columns=(tuple(first_system), *rows.columns[1:]),
        describe=(tuple(first_description), *rows.describe[1:]),
    )
    inspector = SchemaInspector(_FakeGateway(drifted))
    contract = SchemaContract(tables=(METADATA_CONTRACT,))

    raw_difference = await inspector.diff(contract)
    raw_values = tuple(mismatch.actual for mismatch in raw_difference.mismatches)
    with pytest.raises(_PhysicalSchemaDriftError) as captured:
        await inspector.validate(contract)

    error = captured.value
    expected_paths = (
        "columns[0].default_expression",
        "columns[0].describe.default_expression",
        "columns[0].describe.comment",
    )
    assert raw_values == (SECRET_CATALOG_VALUE,) * len(expected_paths)
    assert error.report.mismatch_count == len(expected_paths)
    assert tuple(location.path for location in error.report.locations) == expected_paths
    assert all(location.table == METADATA_TABLE for location in error.report.locations)
    assert not hasattr(error, "difference")
    assert SECRET_CATALOG_VALUE not in str(error)
    assert SECRET_CATALOG_VALUE not in repr(error)
    assert SECRET_CATALOG_VALUE not in str(error.report)
    assert SECRET_CATALOG_VALUE not in repr(error.report)
    production_locals = _traceback_locals(error, module_suffix="/_schema/inspection.py")
    assert SECRET_CATALOG_VALUE not in repr(production_locals)


@pytest.mark.asyncio
async def test_explicit_additive_column_allowlist_is_optional_and_exact() -> None:
    """Accept only an appended allowlisted metadata column with an explicit default."""
    base = METADATA_CONTRACT
    additive = ColumnContract(
        name=Identifier("metadata_format"),
        type_name="UInt8",
        default_kind="DEFAULT",
        default_expression="1",
    )
    rows = _exact_rows((*base.columns, additive))
    strict_difference = await SchemaInspector(_FakeGateway(rows)).diff(SchemaContract(tables=(base,)))
    compatible = replace(base, allowed_additive_columns=(additive,))

    assert {mismatch.path for mismatch in strict_difference.mismatches} == {
        "columns.system_count",
        "columns.unexpected",
    }
    assert await SchemaInspector(_FakeGateway(rows)).matches(SchemaContract(tables=(compatible,)))

    second_additive = ColumnContract(
        name=Identifier("a_format"),
        type_name="UInt8",
        default_kind="DEFAULT",
        default_expression="2",
    )
    compatible = replace(
        base,
        allowed_additive_columns=(additive, second_additive),
    )
    physical_order_rows = _exact_rows((*base.columns, additive, second_additive))
    assert await SchemaInspector(_FakeGateway(physical_order_rows)).matches(SchemaContract(tables=(compatible,)))

    wrong_default = replace(additive, default_expression="9")
    wrong_rows = _exact_rows((*base.columns, wrong_default, second_additive))
    difference = await SchemaInspector(_FakeGateway(wrong_rows)).diff(SchemaContract(tables=(compatible,)))
    assert "columns[10].default_expression" in {mismatch.path for mismatch in difference.mismatches}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("physical_additive", "path_suffix"),
    [
        (replace(COMPATIBLE_ADDITIVE, type_name="UInt16"), "type"),
        (replace(COMPATIBLE_ADDITIVE, default_kind="MATERIALIZED"), "default_kind"),
        (replace(COMPATIBLE_ADDITIVE, default_expression="2"), "default_expression"),
        (replace(COMPATIBLE_ADDITIVE, compression_codec="CODEC(ZSTD(1))"), "compression_codec"),
        (replace(COMPATIBLE_ADDITIVE, comment="not-compatible"), "describe.comment"),
    ],
)
async def test_allowlisted_additive_column_requires_every_exact_fact(
    physical_additive: ColumnContract,
    path_suffix: str,
) -> None:
    """Reject an allowlisted name when any physical compatibility fact differs."""
    compatible = replace(METADATA_CONTRACT, allowed_additive_columns=(COMPATIBLE_ADDITIVE,))
    rows = _exact_rows((*METADATA_CONTRACT.columns, physical_additive))

    difference = await SchemaInspector(_FakeGateway(rows)).diff(SchemaContract(tables=(compatible,)))

    assert f"columns[10].{path_suffix}" in {mismatch.path for mismatch in difference.mismatches}


@pytest.mark.asyncio
async def test_column_position_order_and_column_ttl_are_owned_facts() -> None:
    """Reject reordered columns, inconsistent positions and any undeclared column TTL."""
    rows = _exact_rows(METADATA_CONTRACT.columns)
    first_system = list(rows.columns[1])
    second_system = list(rows.columns[0])
    first_system[0] = 1
    second_system[0] = 3
    first_describe = rows.describe[1]
    second_describe = list(rows.describe[0])
    second_describe[6] = b"recorded_at"
    drifted = replace(
        rows,
        columns=(tuple(first_system), tuple(second_system), *rows.columns[2:]),
        describe=(first_describe, tuple(second_describe), *rows.describe[2:]),
    )

    difference = await SchemaInspector(_FakeGateway(drifted)).diff(SchemaContract(tables=(METADATA_CONTRACT,)))
    paths = {mismatch.path for mismatch in difference.mismatches}

    assert "columns.system_order" in paths
    assert "columns[1].position" in paths
    assert "columns[1].describe.ttl_expression" in paths


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("table_index", "physical_value", "expected_path"),
    [
        (2, b"toYYYYMM(recorded_at)", "partition_key"),
        (5, b"record_kind", "sampling_key"),
        (
            6,
            b"CREATE TABLE inspection_test.taskiq_clickhouse_metadata "
            b"(record_kind String) ENGINE=MergeTree TTL recorded_at DELETE",
            "ttl_expression",
        ),
    ],
)
async def test_metadata_rejects_partition_sampling_and_table_ttl(
    table_index: int,
    physical_value: bytes,
    expected_path: str,
) -> None:
    """Enforce the fixed metadata table's empty partition, sampling and TTL facts."""
    rows = _exact_rows(METADATA_CONTRACT.columns)
    table_row = list(rows.table[0])
    table_row[table_index] = physical_value
    drifted = replace(rows, table=(tuple(table_row),))

    difference = await SchemaInspector(_FakeGateway(drifted)).diff(SchemaContract(tables=(METADATA_CONTRACT,)))

    assert expected_path in {mismatch.path for mismatch in difference.mismatches}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("table_index", "physical_value", "expected_path"),
    [
        (
            7,
            b"CREATE TABLE inspection_test.taskiq_clickhouse_metadata\n"
            b"(\n"
            b"    `record_kind` String,\n"
            b"    CONSTRAINT reject_inserts CHECK 0\n"
            b")\n"
            b"ENGINE = MergeTree",
            "auxiliary.constraints",
        ),
        (9, 1, "auxiliary.data_skipping_indices"),
        (10, 1, "auxiliary.projections"),
        (11, 1, "auxiliary.materialized_views"),
    ],
)
async def test_auxiliary_table_objects_are_explicit_v01_drift(
    table_index: int,
    physical_value: object,
    expected_path: str,
) -> None:
    """Reject every table-level object intentionally excluded from v0.1."""
    rows = _exact_rows(METADATA_CONTRACT.columns)
    table_row = list(rows.table[0])
    table_row[table_index] = physical_value
    drifted = replace(rows, table=(tuple(table_row),))

    difference = await SchemaInspector(_FakeGateway(drifted)).diff(SchemaContract(tables=(METADATA_CONTRACT,)))

    assert {mismatch.path for mismatch in difference.mismatches} == {expected_path}


@pytest.mark.asyncio
async def test_constraint_marker_ignores_identifiers_literals_and_comments() -> None:
    """Trust AST formatting boundaries instead of keyword substring matches."""
    rows = _exact_rows(METADATA_CONTRACT.columns)
    table_row = list(rows.table[0])
    table_row[7] = (
        b"CREATE TABLE inspection_test.taskiq_clickhouse_metadata\n"
        b"(\n"
        b"    `constraint` String DEFAULT 'CONSTRAINT' COMMENT 'constraint'\n"
        b")\n"
        b"ENGINE = MergeTree\n"
        b"COMMENT 'CONSTRAINT'"
    )
    inspector = SchemaInspector(_FakeGateway(replace(rows, table=(tuple(table_row),))))

    assert await inspector.matches(SchemaContract(tables=(METADATA_CONTRACT,)))


@pytest.mark.asyncio
async def test_partial_columns_and_unowned_empty_settings_are_structured_drift() -> None:
    """Keep incomplete catalog state inspectable instead of indexing past available rows."""
    contract = METADATA_CONTRACT
    rows = _exact_rows(contract.columns[:1])
    table_row = list(rows.table[0])
    table_row[1] = b"MergeTree"
    rows = replace(rows, table=(tuple(table_row),))
    inspector = SchemaInspector(_FakeGateway(rows))

    snapshot = await inspector.inspect(SchemaContract(tables=(contract,)))
    difference = compare_schema(snapshot, SchemaContract(tables=(contract,)))

    assert snapshot.table(METADATA_TABLE) is not None
    assert snapshot.tables[0].settings == ()
    assert "columns.system_count" in {mismatch.path for mismatch in difference.mismatches}


@pytest.mark.asyncio
async def test_sql_clause_parser_preserves_quoted_content_and_nested_settings() -> None:
    """Normalize owned clauses without splitting commas or whitespace inside expressions."""
    contract = METADATA_CONTRACT
    rows = _exact_rows(contract.columns)
    table_row = list(rows.table[0])
    table_row[1] = b"MergeTree ORDER BY tuple() SETTINGS alpha = concat('a,  b', 'c\\'d'), beta = tuple(1, 2)"
    table_row[6] = (
        b"CREATE TABLE inspection_test.taskiq_clickhouse_metadata "
        b"(x String DEFAULT concat('TTL  ignored', tuple('a', 'b'))) "
        b"ENGINE=MergeTree TTL recorded_at DELETE COMMENT 'SETTINGS  ignored' "
        b"SETTINGS index_granularity=8192"
    )
    snapshot = await SchemaInspector(_FakeGateway(replace(rows, table=(tuple(table_row),)))).inspect(
        SchemaContract(tables=(contract,))
    )
    physical = snapshot.tables[0]

    assert physical.ttl_expression == "recorded_at"
    assert physical.settings == (
        ("alpha", "concat('a,  b', 'c\\'d')"),
        ("beta", "tuple(1, 2)"),
    )
    assert SQL_EXPRESSION_PARSER.table_ttl("ENGINE=MergeTree TTL recorded_at + INTERVAL 1 DAY") == (
        "recorded_at + INTERVAL 1 DAY"
    )
    assert SQL_EXPRESSION_PARSER.table_ttl("ENGINE=MergeTree TTL first DELETE, second DELETE") == (
        "first DELETE, second DELETE"
    )


@pytest.mark.parametrize(
    "formatted_create_query",
    [
        "CREATE VIEW inspection_test.example AS SELECT 1",
        "CREATE TABLE inspection_test.example\n(\n    value UInt8\nENGINE = MergeTree",
    ],
    ids=["wrong-statement", "missing-column-list-close"],
)
def test_constraint_parser_rejects_unsupported_formatted_create_shapes(
    formatted_create_query: str,
) -> None:
    """Fail closed when server formatting no longer has the owned table shape."""
    with pytest.raises(SchemaInspectionError, match="unsupported CREATE TABLE shape"):
        SQL_EXPRESSION_PARSER.has_constraints(formatted_create_query)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("rows", "message"),
    [
        (
            _PhysicalRows(
                table=((b"MergeTree",) * 12, (b"MergeTree",) * 12),
                columns=(),
                describe=(),
            ),
            "multiple rows",
        ),
        (_PhysicalRows(table=((b"MergeTree",),), columns=(), describe=()), "fields instead"),
        (
            _PhysicalRows(
                table=((b"\xff", b"", b"", b"", b"", b"", b"", b"", b"", 0, 0, 0),),
                columns=(),
                describe=(),
            ),
            "not valid UTF-8",
        ),
        (
            _PhysicalRows(
                table=(
                    (
                        b"MergeTree",
                        b"MergeTree SETTINGS malformed",
                        b"",
                        b"x",
                        b"x",
                        b"",
                        b"CREATE TABLE inspection_test.taskiq_clickhouse_metadata (x UInt8) ENGINE=MergeTree",
                        DEFAULT_FORMATTED_CREATE_QUERY.encode(),
                        DEFAULT_FORMATTED_CONSTRAINT_PROBE.encode(),
                        0,
                        0,
                        0,
                    ),
                ),
                columns=(),
                describe=(),
            ),
            "malformed SETTINGS",
        ),
        (
            _PhysicalRows(
                table=(
                    (
                        b"MergeTree",
                        b"MergeTree",
                        b"",
                        b"x",
                        b"x",
                        b"",
                        b"CREATE TABLE inspection_test.taskiq_clickhouse_metadata (x UInt8) ENGINE=MergeTree",
                        b"CREATE TABLE inspection_test.taskiq_clickhouse_metadata (x UInt8) ENGINE = MergeTree",
                        DEFAULT_FORMATTED_CONSTRAINT_PROBE.encode(),
                        0,
                        0,
                        0,
                    ),
                ),
                columns=(),
                describe=(),
            ),
            "unsupported CREATE TABLE shape",
        ),
        (
            _PhysicalRows(
                table=(
                    (
                        b"MergeTree",
                        b"MergeTree",
                        b"",
                        b"x",
                        b"x",
                        b"",
                        b"CREATE TABLE inspection_test.taskiq_clickhouse_metadata (x UInt8) ENGINE=MergeTree",
                        DEFAULT_FORMATTED_CREATE_QUERY.encode(),
                        DEFAULT_FORMATTED_CREATE_QUERY.encode(),
                        0,
                        0,
                        0,
                    ),
                ),
                columns=(),
                describe=(),
            ),
            "constraint capability is unsupported",
        ),
    ],
)
async def test_malformed_table_catalog_responses_fail_closed(
    rows: _PhysicalRows,
    message: str,
) -> None:
    """Reject malformed table-catalog responses instead of constructing partial facts."""
    inspector = SchemaInspector(_FakeGateway(rows))
    contract = SchemaContract(tables=(METADATA_CONTRACT,))

    with pytest.raises(SchemaInspectionError, match=message):
        await inspector.inspect(contract)


@pytest.mark.asyncio
@pytest.mark.parametrize("table_index", [9, 10, 11])
@pytest.mark.parametrize("physical_flag", [True, -1, 2, "1", b"1"])
async def test_malformed_auxiliary_catalog_flags_fail_closed(
    table_index: int,
    physical_flag: object,
) -> None:
    """Reject driver coercion and non-binary EXISTS projections."""
    rows = _exact_rows(())
    table_row = list(rows.table[0])
    table_row[table_index] = physical_flag
    inspector = SchemaInspector(_FakeGateway(replace(rows, table=(tuple(table_row),))))

    with pytest.raises(SchemaInspectionError, match="binary flag"):
        await inspector.inspect(SchemaContract(tables=(METADATA_CONTRACT,)))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("column_row", "describe_row", "message"),
    [
        ((1,), (), "fields instead"),
        ((0, b"x", b"UInt8", b"", b"", b""), (), "positive integer"),
        ((True, b"x", b"UInt8", b"", b"", b""), (), "positive integer"),
        (("1", b"x", b"UInt8", b"", b"", b""), (), "positive integer"),
        ((1, "x", b"UInt8", b"", b"", b""), (), "exact byte string"),
        (
            (1, b"x", b"UInt8", b"", b"", b""),
            (b"x",),
            "fields instead",
        ),
    ],
)
async def test_malformed_column_catalog_responses_fail_closed(
    column_row: tuple[object, ...],
    describe_row: tuple[object, ...],
    message: str,
) -> None:
    """Reject invalid positions, bytes and DESCRIBE shapes."""
    base_rows = _exact_rows(())
    rows = replace(
        base_rows,
        columns=(column_row,),
        describe=(describe_row,) if describe_row else (),
    )
    inspector = SchemaInspector(_FakeGateway(rows))
    contract = SchemaContract(tables=(METADATA_CONTRACT,))

    with pytest.raises(SchemaInspectionError, match=message):
        await inspector.inspect(contract)


def _exact_rows(columns: Sequence[ColumnContract]) -> _PhysicalRows:
    system_rows = tuple(
        (
            position,
            column.name.value.encode(),
            column.type_name.encode(),
            column.default_kind.encode(),
            column.default_expression.encode(),
            column.compression_codec.encode(),
        )
        for position, column in enumerate(columns, start=1)
    )
    describe_rows = tuple(
        (
            column.name.value.encode(),
            column.type_name.encode(),
            column.default_kind.encode(),
            column.default_expression.encode(),
            column.comment.encode(),
            column.compression_codec.encode(),
            column.ttl_expression.encode(),
        )
        for column in columns
    )
    return _PhysicalRows(
        table=(
            (
                b"MergeTree",
                DEFAULT_ENGINE_FULL.encode(),
                b"",
                b"record_kind, scope, record_key, version, checksum, recorded_at, attempt_id",
                b"record_kind, scope, record_key, version",
                b"",
                DEFAULT_CREATE_QUERY.encode(),
                DEFAULT_FORMATTED_CREATE_QUERY.encode(),
                DEFAULT_FORMATTED_CONSTRAINT_PROBE.encode(),
                0,
                0,
                0,
            ),
        ),
        columns=system_rows,
        describe=describe_rows,
    )


def _traceback_locals(
    error: BaseException,
    *,
    module_suffix: str,
) -> tuple[dict[str, object], ...]:
    """Capture only production-frame locals retained by one exception."""
    captured: list[dict[str, object]] = []
    traceback_node = error.__traceback__
    while traceback_node is not None:
        frame = traceback_node.tb_frame
        if frame.f_code.co_filename.endswith(module_suffix):
            captured.append(dict(frame.f_locals))
        traceback_node = traceback_node.tb_next
    return tuple(captured)
