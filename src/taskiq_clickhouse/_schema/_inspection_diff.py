"""Structured comparison of physical and expected schema snapshots."""

from taskiq_clickhouse._identifiers import QualifiedTable
from taskiq_clickhouse._schema._inspection_diff_types import (
    SchemaDifference,
    SchemaMismatch,
)
from taskiq_clickhouse._schema._inspection_types import (
    AuxiliaryObjectsSnapshot,
    DescribedColumnSnapshot,
    SchemaSnapshot,
    SystemColumnSnapshot,
    TableSnapshot,
)
from taskiq_clickhouse._schema.contracts import ColumnContract, SchemaContract, TableContract


_ExpectedColumns = tuple[ColumnContract, ...]
_SchemaMismatches = tuple[SchemaMismatch, ...]
_ExpectedColumnsResult = tuple[_ExpectedColumns, _SchemaMismatches]
_ColumnNames = tuple[str, ...]
_AdditionPartition = tuple[_ExpectedColumns, _ColumnNames]


class _DifferenceBuilder:
    """Accumulate safe physical mismatches for one complete contract."""

    def __init__(self) -> None:
        self._mismatches: list[SchemaMismatch] = []

    def compare(self, snapshot: SchemaSnapshot, contract: SchemaContract) -> SchemaDifference:
        """Compare all required-present and required-absent tables."""
        self._mismatches.clear()
        for table_contract in contract.tables:
            table_snapshot = snapshot.table(table_contract.table)
            if table_snapshot is None:
                self._record(table_contract.table, "table", "present", "absent")
            else:
                self._compare_table(table_snapshot, table_contract)
        for absent_table in contract.absent_tables:
            if snapshot.table(absent_table) is not None:
                self._record(absent_table, "table", "absent", "present")
        return SchemaDifference(mismatches=tuple(self._mismatches))

    def _compare_table(self, actual: TableSnapshot, expected: TableContract) -> None:
        for path, expected_expression, actual_expression in (
            ("engine", expected.engine, actual.engine),
            ("partition_key", expected.partition_key, actual.partition_key),
            ("sorting_key", expected.sorting_key, actual.sorting_key),
            ("primary_key", expected.primary_key, actual.primary_key),
            ("sampling_key", expected.sampling_key, actual.sampling_key),
            ("ttl_expression", expected.ttl_expression, actual.ttl_expression),
        ):
            self._record(actual.table, path, expected_expression, actual_expression)
        actual_settings = dict(actual.settings)
        for setting_name, expected_expression in expected.critical_settings:
            self._record(
                actual.table,
                f"settings.{setting_name}",
                expected_expression,
                actual_settings.get(setting_name),
            )
        self._mismatches.extend(
            _mismatches(
                actual.table,
                "auxiliary",
                _auxiliary_absence_comparisons(actual.auxiliary_objects),
            )
        )
        self._compare_columns(actual, expected)

    def _compare_columns(
        self,
        actual: TableSnapshot,
        expected: TableContract,
    ) -> None:
        system_names = tuple(column.name for column in actual.columns)
        self._mismatches.extend(_column_order_mismatches(actual, expected, system_names))
        expected_columns, additive_mismatches = _expected_columns(actual, expected)
        self._mismatches.extend(additive_mismatches)
        self._compare_column_rows(actual, expected_columns)
        self._record(
            actual.table,
            "columns.system_count",
            len(expected_columns),
            len(actual.columns),
        )
        self._record(
            actual.table,
            "columns.describe_count",
            len(actual.columns),
            len(actual.described_columns),
        )

    def _compare_column_rows(
        self,
        table: TableSnapshot,
        expected_columns: tuple[ColumnContract, ...],
    ) -> None:
        physical_pairs = zip(table.columns, table.described_columns, strict=False)
        contract_pairs = zip(expected_columns, physical_pairs, strict=False)
        for index, (expected, physical) in enumerate(contract_pairs):
            self._compare_column(table.table, index, expected, physical)

    def _compare_column(
        self,
        table: QualifiedTable,
        index: int,
        expected: ColumnContract,
        physical: tuple[SystemColumnSnapshot, DescribedColumnSnapshot],
    ) -> None:
        system_column, described_column = physical
        prefix = f"columns[{index}]"
        comparisons = (
            ("position", index + 1, system_column.position),
            ("name", expected.name.value, system_column.name),
            ("type", expected.type_name, system_column.type_name),
            ("default_kind", expected.default_kind, system_column.default_kind),
            ("default_expression", expected.default_expression, system_column.default_expression),
            ("compression_codec", expected.compression_codec, system_column.compression_codec),
            ("describe.name", system_column.name, described_column.name),
            ("describe.type", system_column.type_name, described_column.type_name),
            ("describe.default_kind", expected.default_kind, described_column.default_kind),
            (
                "describe.default_expression",
                expected.default_expression,
                described_column.default_expression,
            ),
            ("describe.comment", expected.comment, described_column.comment),
            (
                "describe.compression_codec",
                expected.compression_codec,
                described_column.compression_codec,
            ),
            ("describe.ttl_expression", expected.ttl_expression, described_column.ttl_expression),
        )
        self._mismatches.extend(_mismatches(table, prefix, comparisons))

    def _record(
        self,
        table: QualifiedTable,
        path: str,
        expected: object,
        actual: object,
    ) -> None:
        if expected != actual:
            self._mismatches.append(
                SchemaMismatch(
                    table=table,
                    path=path,
                    expected=expected,
                    actual=actual,
                )
            )


def _mismatches(
    table: QualifiedTable,
    prefix: str,
    comparisons: tuple[tuple[str, object, object], ...],
) -> tuple[SchemaMismatch, ...]:
    return tuple(
        SchemaMismatch(
            table=table,
            path=f"{prefix}.{path}" if prefix else path,
            expected=expected,
            actual=actual,
        )
        for path, expected, actual in comparisons
        if expected != actual
    )


def _auxiliary_absence_comparisons(
    snapshot: AuxiliaryObjectsSnapshot,
) -> tuple[tuple[str, object, object], ...]:
    """Project v0.1's required absence into generic comparisons."""
    return (
        ("constraints", False, snapshot.constraints),
        ("data_skipping_indices", False, snapshot.data_skipping_indices),
        ("materialized_views", False, snapshot.materialized_views),
        ("projections", False, snapshot.projections),
    )


def _column_order_mismatches(
    actual: TableSnapshot,
    expected: TableContract,
    system_names: tuple[str, ...],
) -> tuple[SchemaMismatch, ...]:
    base_names = tuple(column.name.value for column in expected.columns)
    described_names = tuple(column.name for column in actual.described_columns)
    comparisons = (
        ("columns.system_order", base_names, system_names[: len(base_names)]),
        ("columns.describe_order", system_names, described_names),
    )
    return _mismatches(actual.table, "", comparisons)


def _expected_columns(
    actual: TableSnapshot,
    expected: TableContract,
) -> _ExpectedColumnsResult:
    system_names = tuple(column.name for column in actual.columns)
    additions, unexpected_names = _partition_additions(
        {column.name.value: column for column in expected.allowed_additive_columns},
        system_names[len(expected.columns) :],
    )
    return expected.columns + additions, _mismatches(
        actual.table,
        "",
        (("columns.unexpected", (), unexpected_names),),
    )


def _partition_additions(
    additive_by_name: dict[str, ColumnContract],
    extra_names: _ColumnNames,
) -> _AdditionPartition:
    additions: list[ColumnContract] = []
    unexpected_names: list[str] = []
    for name in extra_names:
        addition = additive_by_name.get(name)
        if addition is None:
            unexpected_names.append(name)
        else:
            additions.append(addition)
    return tuple(additions), tuple(unexpected_names)


def compare_schema(snapshot: SchemaSnapshot, contract: SchemaContract) -> SchemaDifference:
    """Compare one captured snapshot with an exact expected schema phase."""
    return _DifferenceBuilder().compare(snapshot, contract)
