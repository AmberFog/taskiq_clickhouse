"""Verify the managed-table definition used for contracts and native inserts."""

from collections.abc import Callable
from dataclasses import replace

import pytest

from taskiq_clickhouse._identifiers import Identifier, QualifiedTable
from taskiq_clickhouse._schema.contracts import ColumnContract
from taskiq_clickhouse._schema.table_definition import (
    TableDefinition,
    columns,
)


_TABLE = QualifiedTable(Identifier("analytics"), Identifier("events"))
_COLUMNS = columns(
    ("namespace", "String"),
    ("event_id", "UUID"),
    ("purge_at", "DateTime64(6, 'UTC')"),
)
_DEFINITION = TableDefinition(
    columns=_COLUMNS,
    engine="MergeTree",
    partition_key="toYYYYMM(purge_at)",
    primary_key=("namespace",),
    sorting_key=("namespace", "event_id"),
    ttl_expression="purge_at",
)


def test_definition_exposes_one_native_insert_and_contract_order() -> None:
    """Derive native columns and the physical contract from one definition."""
    contract = _DEFINITION.contract_for(_TABLE)

    assert _DEFINITION.column_names == ("namespace", "event_id", "purge_at")
    assert _DEFINITION.column_types == ("String", "UUID", "DateTime64(6, 'UTC')")
    assert contract.table == _TABLE
    assert contract.primary_key == "namespace"
    assert contract.sorting_key == "namespace, event_id"
    assert contract.ttl_expression == "purge_at"


@pytest.mark.parametrize(
    ("factory", "message"),
    [
        pytest.param(
            lambda: replace(_DEFINITION, primary_key=()),
            "primary key must not be empty",
            id="empty-primary-key",
        ),
        pytest.param(
            lambda: replace(_DEFINITION, sorting_key=("namespace", "namespace")),
            "sorting key must not contain duplicates",
            id="duplicate-sorting-key",
        ),
        pytest.param(
            lambda: replace(_DEFINITION, sorting_key=("namespace", " namespace ")),
            "sorting key must not contain duplicates",
            id="duplicate-normalized-sorting-key",
        ),
        pytest.param(
            lambda: replace(_DEFINITION, primary_key=("missing",)),
            "primary key must reference declared columns",
            id="unknown-primary-key",
        ),
        pytest.param(
            lambda: replace(_DEFINITION, sorting_key=("namespace", "missing")),
            "sorting key must reference declared columns",
            id="unknown-sorting-key",
        ),
        pytest.param(
            lambda: replace(
                _DEFINITION,
                primary_key=("event_id",),
                sorting_key=("namespace", "event_id"),
            ),
            "primary key must be a sorting key prefix",
            id="primary-key-not-prefix",
        ),
    ],
)
def test_definition_rejects_invalid_keys(
    factory: Callable[[], TableDefinition],
    message: str,
) -> None:
    """Require normalized declared keys and a valid primary-key prefix."""
    with pytest.raises(ValueError, match=message):
        factory()


def test_definition_rejects_empty_or_duplicate_columns() -> None:
    """Prevent an incomplete or ambiguous native row contract."""
    with pytest.raises(ValueError, match="must declare columns"):
        replace(_DEFINITION, columns=())
    with pytest.raises(ValueError, match="must not contain duplicates"):
        replace(_DEFINITION, columns=(_COLUMNS[0], _COLUMNS[0]))


@pytest.mark.parametrize(
    "column",
    [
        ColumnContract(Identifier("value"), "String", default_kind="DEFAULT"),
        ColumnContract(Identifier("value"), "String", default_expression="''"),
        ColumnContract(Identifier("value"), "String", compression_codec="CODEC(ZSTD(1))"),
        ColumnContract(Identifier("value"), "String", comment="managed"),
    ],
)
def test_definition_rejects_unsupported_column_facts(column: ColumnContract) -> None:
    """Keep bootstrap DDL aligned with the deliberately supported subset."""
    with pytest.raises(ValueError, match="unsupported physical facts"):
        replace(_DEFINITION, columns=(column,))
