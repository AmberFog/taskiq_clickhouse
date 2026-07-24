"""Unit tests for the immutable production storage migration."""

from dataclasses import replace
from typing import Final, cast

import pytest

from taskiq_clickhouse._identifiers import Identifier, QualifiedTable
from taskiq_clickhouse._schema.contracts import SchemaContract, TableContract
from taskiq_clickhouse._storage.layout import (
    MIGRATION_V1_NAME,
    PROGRESS_COLUMN_NAMES,
    PROGRESS_COLUMN_TYPES,
    RESULT_COLUMN_NAMES,
    RESULT_COLUMN_TYPES,
    StorageLayout,
    build_storage_plan,
    storage_layout_from_names,
)
from taskiq_clickhouse._types import MigrationExecution


DATABASE: Final = Identifier("analytics")
RESULT_TABLE: Final = QualifiedTable(DATABASE, Identifier("task_results"))
PROGRESS_TABLE: Final = QualifiedTable(DATABASE, Identifier("task_progress"))
EXPECTED_MIGRATION_CHECKSUM: Final = (
    "7bea9fc16db59c255467b9b68f3eb98c2372f7ac914e1b4a4ec9df0ab77e0720"  # pragma: allowlist secret
)
LAYOUT: Final = StorageLayout(
    result_table=RESULT_TABLE,
    progress_table=PROGRESS_TABLE,
)

EXPECTED_RESULT_DDL: Final = """CREATE TABLE IF NOT EXISTS {database:Identifier}.{table:Identifier}
(
    `namespace` String,
    `task_id` String,
    `generation_at` DateTime64(6, 'UTC'),
    `generation_id` UUID,
    `state` UInt8,
    `written_at` DateTime64(6, 'UTC'),
    `visible_until` DateTime64(6, 'UTC'),
    `purge_at` DateTime64(6, 'UTC'),
    `result_payload` String,
    `log_payload` String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(purge_at)
PRIMARY KEY (namespace, task_id)
ORDER BY
(
    namespace,
    task_id,
    generation_at,
    generation_id,
    state
)
TTL purge_at DELETE"""
EXPECTED_PROGRESS_DDL: Final = """CREATE TABLE IF NOT EXISTS {database:Identifier}.{table:Identifier}
(
    `namespace` String,
    `task_id` String,
    `generation_at` DateTime64(6, 'UTC'),
    `generation_id` UUID,
    `written_at` DateTime64(6, 'UTC'),
    `visible_until` DateTime64(6, 'UTC'),
    `purge_at` DateTime64(6, 'UTC'),
    `progress_payload` String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(purge_at)
PRIMARY KEY (namespace, task_id)
ORDER BY
(
    namespace,
    task_id,
    generation_at,
    generation_id
)
TTL purge_at DELETE"""


def test_migration_v1_has_exact_retry_safe_ddl_and_policy() -> None:
    """Freeze both statements, their order and AUTO safety classification."""
    plan = build_storage_plan(LAYOUT)
    migration = plan.migrations[0]

    assert plan.target_version == 1
    assert migration.version == 1
    assert migration.name == MIGRATION_V1_NAME == "create_result_and_progress_tables"
    assert migration.execution is MigrationExecution.AUTO
    assert migration.reentrant is True
    assert migration.concurrent_safe is True
    assert tuple(step.ddl for step in migration.steps) == (
        EXPECTED_RESULT_DDL,
        EXPECTED_PROGRESS_DDL,
    )
    assert tuple(dict(step.query_parameters) for step in migration.steps) == (
        {"database": "analytics", "table": "task_results"},
        {"database": "analytics", "table": "task_progress"},
    )
    assert all("ON CLUSTER" not in step.ddl for step in migration.steps)
    assert all("SETTINGS" not in step.ddl for step in migration.steps)
    assert all(step.ddl.count("CREATE TABLE IF NOT EXISTS") == 1 for step in migration.steps)
    assert all(step.ddl.count("TTL purge_at DELETE") == 1 for step in migration.steps)


@pytest.mark.parametrize(
    ("contract", "column_names", "column_types", "sorting_key"),
    [
        (
            LAYOUT.result_contract,
            RESULT_COLUMN_NAMES,
            RESULT_COLUMN_TYPES,
            "namespace, task_id, generation_at, generation_id, state",
        ),
        (
            LAYOUT.progress_contract,
            PROGRESS_COLUMN_NAMES,
            PROGRESS_COLUMN_TYPES,
            "namespace, task_id, generation_at, generation_id",
        ),
    ],
)
def test_storage_contract_pins_every_owned_physical_fact(
    contract: TableContract,
    column_names: tuple[str, ...],
    column_types: tuple[str, ...],
    sorting_key: str,
) -> None:
    """Keep only package-owned columns, keys and TTL immutable."""
    assert tuple(column.name.value for column in contract.columns) == column_names
    assert tuple(column.type_name for column in contract.columns) == column_types
    assert contract.engine == "MergeTree"
    assert contract.partition_key == "toYYYYMM(purge_at)"
    assert contract.primary_key == "namespace, task_id"
    assert contract.sorting_key == sorting_key
    assert contract.sampling_key == ""
    assert contract.ttl_expression == "purge_at"
    assert contract.critical_settings == ()
    assert contract.allowed_additive_columns == ()
    assert all(column.default_kind == "" for column in contract.columns)
    assert all(column.default_expression == "" for column in contract.columns)
    assert all(column.compression_codec == "" for column in contract.columns)
    assert all(column.comment == "" for column in contract.columns)
    assert all(column.ttl_expression == "" for column in contract.columns)


def test_migration_steps_encode_every_recoverable_physical_phase() -> None:
    """Represent empty, result-ready and fully-ready states without race drift."""
    first, second = build_storage_plan(LAYOUT).migrations[0].steps
    empty = SchemaContract(absent_tables=(RESULT_TABLE, PROGRESS_TABLE))
    result_ready = SchemaContract(tables=(LAYOUT.result_contract,))
    storage_ready = LAYOUT.target_contract

    assert first.before == empty
    assert first.after == result_ready
    assert second.before == result_ready
    assert second.after == storage_ready
    assert first.after.absent_tables == ()
    assert second.before.absent_tables == ()
    assert first.after.tables == (LAYOUT.result_contract,)
    assert second.after.tables == tuple(sorted(storage_ready.tables, key=lambda contract: contract.table.canonical))
    assert build_storage_plan(LAYOUT).migrations[0].target == storage_ready


def test_migration_descriptor_is_deterministic_and_identifier_scoped() -> None:
    """Freeze permanent history while retaining validated dynamic table names."""
    first = build_storage_plan(LAYOUT).migrations[0]
    second = build_storage_plan(LAYOUT).migrations[0]
    alternate_layout = StorageLayout(
        result_table=QualifiedTable(Identifier("tenant_42"), Identifier("select")),
        progress_table=QualifiedTable(Identifier("tenant_42"), Identifier("progress_2026")),
    )
    alternate = build_storage_plan(alternate_layout).migrations[0]

    assert first == second
    assert first.payload_bytes == second.payload_bytes
    assert first.checksum == second.checksum
    assert first.checksum == EXPECTED_MIGRATION_CHECKSUM
    assert alternate.checksum != first.checksum
    assert alternate_layout.database == Identifier("tenant_42")
    assert alternate.steps[0].query_parameters == {
        "database": "tenant_42",
        "table": "select",
    }
    assert alternate.steps[1].query_parameters == {
        "database": "tenant_42",
        "table": "progress_2026",
    }
    assert alternate.target.tables[0].table.canonical == "tenant_42.progress_2026"
    assert alternate.target.tables[1].table.canonical == "tenant_42.select"


def test_storage_layout_rejects_unsafe_or_inconsistent_tables() -> None:
    """Fail before SQL construction for malformed layout components."""
    with pytest.raises(TypeError, match="result table"):
        StorageLayout(
            result_table=cast("QualifiedTable", object()),
            progress_table=PROGRESS_TABLE,
        )
    with pytest.raises(TypeError, match="progress table"):
        StorageLayout(
            result_table=RESULT_TABLE,
            progress_table=cast("QualifiedTable", object()),
        )
    with pytest.raises(ValueError, match="must differ"):
        StorageLayout(result_table=RESULT_TABLE, progress_table=RESULT_TABLE)
    with pytest.raises(ValueError, match="share one database"):
        StorageLayout(
            result_table=RESULT_TABLE,
            progress_table=QualifiedTable(Identifier("other"), Identifier("task_progress")),
        )
    with pytest.raises(ValueError, match="identifier contract"):
        Identifier("unsafe`; DROP TABLE x")
    with pytest.raises(TypeError, match="storage layout"):
        build_storage_plan(cast("StorageLayout", object()))

    assert storage_layout_from_names("analytics", "task_results", "task_progress") == LAYOUT
    with pytest.raises(ValueError, match="identifier contract"):
        storage_layout_from_names("unsafe-db", "task_results", "task_progress")


def test_contract_checksum_changes_for_any_owned_physical_fact() -> None:
    """Cover the full postcondition inside permanent migration history."""
    original = build_storage_plan(LAYOUT).migrations[0]
    drifted_result = replace(
        LAYOUT.result_contract,
        sorting_key="namespace, task_id, generation_at, generation_id",
    )
    drifted_target = SchemaContract(
        tables=(drifted_result, LAYOUT.progress_contract),
    )
    first_step, second_step = original.steps
    drifted_migration = replace(
        original,
        steps=(first_step, replace(second_step, after=drifted_target)),
    )

    assert drifted_target.canonical_data() != original.target.canonical_data()
    assert drifted_migration.checksum != original.checksum
