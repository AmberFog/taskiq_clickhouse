"""Pin the exact parameterized SQL shapes of the storage boundary."""

from datetime import UTC, datetime, timedelta
from types import MappingProxyType
from typing import Any, cast
from uuid import UUID

import pytest

from taskiq_clickhouse._identifiers import Identifier, QualifiedTable
from taskiq_clickhouse._storage.bindings import (
    point_parameters,
    progress_confirmation_parameters,
    result_confirmation_parameters,
)
from taskiq_clickhouse._storage.layout import (
    PROGRESS_COLUMN_NAMES,
    PROGRESS_COLUMN_TYPES,
    RESULT_COLUMN_NAMES,
    RESULT_COLUMN_TYPES,
)
from taskiq_clickhouse._storage.progress_records import ProgressRecord
from taskiq_clickhouse._storage.queries import (
    NO_LOG_COLUMN_FORMATS,
    PROGRESS_COLUMN_FORMATS,
    PROGRESS_INSERT_COLUMN_NAMES,
    PROGRESS_INSERT_COLUMN_TYPES,
    RESULT_INSERT_COLUMN_NAMES,
    RESULT_INSERT_COLUMN_TYPES,
    WITH_LOG_COLUMN_FORMATS,
    ProgressQueries,
    ResultQueries,
)
from taskiq_clickhouse._storage.result_records import ResultRecord


DATABASE = Identifier("analytics")
RESULT_TABLE = QualifiedTable(DATABASE, Identifier("task_results"))
PROGRESS_TABLE = QualifiedTable(DATABASE, Identifier("task_progress"))
RESULT_QUERIES = ResultQueries(RESULT_TABLE)
PROGRESS_QUERIES = ProgressQueries(PROGRESS_TABLE)
NOW = datetime(2026, 7, 15, 12, tzinfo=UTC)
GENERATION_ID = UUID("00000000-0000-4000-8000-000000000001")

RESULT_ALLOCATOR_SQL = """SELECT
    now64(6, 'UTC') AS written_at,
    maxOrNull(generation_at) AS latest_generation_at,
    maxOrNull(purge_at) AS latest_purge_at
FROM {database:Identifier}.{table:Identifier}
PREWHERE namespace = {namespace:String} AND task_id = {task_id:String}"""
PROGRESS_ALLOCATOR_SQL = RESULT_ALLOCATOR_SQL
READINESS_SQL = """SELECT now64(6, 'UTC') AS observed_at,
       generation_at, generation_id, state, visible_until, purge_at
FROM {database:Identifier}.{table:Identifier}
PREWHERE namespace = {namespace:String} AND task_id = {task_id:String}
ORDER BY namespace DESC, task_id DESC,
         generation_at DESC, generation_id DESC, state DESC
LIMIT 1"""
NO_LOG_SQL = """SELECT now64(6, 'UTC') AS observed_at,
       generation_at, generation_id, state, visible_until, purge_at,
       result_payload
FROM {database:Identifier}.{table:Identifier}
PREWHERE namespace = {namespace:String} AND task_id = {task_id:String}
ORDER BY namespace DESC, task_id DESC,
         generation_at DESC, generation_id DESC, state DESC
LIMIT 1"""
WITH_LOG_SQL = NO_LOG_SQL.replace("       result_payload\n", "       result_payload, log_payload\n")
PROGRESS_SQL = """SELECT now64(6, 'UTC') AS observed_at,
       generation_at, generation_id, visible_until, purge_at,
       progress_payload
FROM {database:Identifier}.{table:Identifier}
PREWHERE namespace = {namespace:String} AND task_id = {task_id:String}
ORDER BY namespace DESC, task_id DESC,
         generation_at DESC, generation_id DESC
LIMIT 1"""
RESULT_CONFIRMATION_SQL = """SELECT 1
FROM {database:Identifier}.{table:Identifier}
PREWHERE namespace = {namespace:String} AND task_id = {task_id:String}
WHERE generation_at = {generation_at:DateTime64(6, 'UTC')}
  AND generation_id = {generation_id:UUID}
  AND state = {state:UInt8}
  AND written_at = {written_at:DateTime64(6, 'UTC')}
  AND visible_until = {visible_until:DateTime64(6, 'UTC')}
  AND purge_at = {purge_at:DateTime64(6, 'UTC')}
LIMIT 1"""
PROGRESS_CONFIRMATION_SQL = """SELECT 1
FROM {database:Identifier}.{table:Identifier}
PREWHERE namespace = {namespace:String} AND task_id = {task_id:String}
WHERE generation_at = {generation_at:DateTime64(6, 'UTC')}
  AND generation_id = {generation_id:UUID}
LIMIT 1"""


def _result_record() -> ResultRecord:
    return ResultRecord(
        namespace="tenant:blue",
        task_id="task' OR 1 = 1 --",
        generation_at=NOW,
        generation_id=GENERATION_ID,
        state=0,
        written_at=NOW,
        visible_until=NOW + timedelta(hours=1),
        purge_at=NOW + timedelta(days=1),
        result_payload=b"result",
        log_payload=b"log",
    )


def _progress_record() -> ProgressRecord:
    return ProgressRecord(
        namespace="tenant:blue",
        task_id="task' OR 1 = 1 --",
        generation_at=NOW,
        generation_id=GENERATION_ID,
        written_at=NOW,
        visible_until=NOW + timedelta(hours=1),
        purge_at=NOW + timedelta(days=1),
        progress_payload=b"progress",
    )


@pytest.mark.parametrize("query_type", [ResultQueries, ProgressQueries])
def test_storage_queries_require_a_validated_qualified_table(
    query_type: type[ResultQueries | ProgressQueries],
) -> None:
    """Reject raw table text instead of rebuilding identifier invariants."""
    with pytest.raises(TypeError, match="table must be a QualifiedTable"):
        query_type(cast("Any", "results"))


def test_allocator_queries_match_the_frozen_server_time_contract() -> None:
    """Read server now and both point-local retention maxima from each table."""
    assert RESULT_QUERIES.allocator == RESULT_ALLOCATOR_SQL
    assert PROGRESS_QUERIES.allocator == PROGRESS_ALLOCATOR_SQL


def test_read_queries_match_the_frozen_projections_exactly() -> None:
    """Pin metadata-only, no-log, with-log and progress query text."""
    assert RESULT_QUERIES.readiness == READINESS_SQL
    assert RESULT_QUERIES.no_log == NO_LOG_SQL
    assert RESULT_QUERIES.with_log == WITH_LOG_SQL
    assert PROGRESS_QUERIES.latest == PROGRESS_SQL


def test_confirmation_queries_match_complete_logical_identities() -> None:
    """Confirm only the same frozen generation and state that was inserted."""
    assert RESULT_QUERIES.confirmation == RESULT_CONFIRMATION_SQL
    assert PROGRESS_QUERIES.confirmation == PROGRESS_CONFIRMATION_SQL


def test_every_latest_query_uses_the_full_reverse_sorting_prefix() -> None:
    """Preserve ClickHouse 25.8 reverse read-in-order behavior."""
    result_order = "ORDER BY namespace DESC, task_id DESC,\n         generation_at DESC, generation_id DESC, state DESC"
    progress_order = "ORDER BY namespace DESC, task_id DESC,\n         generation_at DESC, generation_id DESC"

    assert all(
        result_order in query for query in (RESULT_QUERIES.readiness, RESULT_QUERIES.no_log, RESULT_QUERIES.with_log)
    )
    assert progress_order in PROGRESS_QUERIES.latest


def test_read_queries_never_prefilter_latest_state_or_visibility() -> None:
    """Keep expiration and tombstone evaluation above latest-row selection."""
    for query in (
        RESULT_QUERIES.readiness,
        RESULT_QUERIES.no_log,
        RESULT_QUERIES.with_log,
        PROGRESS_QUERIES.latest,
    ):
        assert "visible_until >" not in query
        assert "state = 0" not in query
        assert "purge_at >" not in query


def test_no_log_and_readiness_do_not_reference_forbidden_payloads() -> None:
    """Prevent hidden nested projections from reading omitted serialized data."""
    assert "payload" not in RESULT_QUERIES.readiness
    assert "log_payload" not in RESULT_QUERIES.no_log
    assert RESULT_QUERIES.no_log.count("result_payload") == 1
    assert RESULT_QUERIES.with_log.count("result_payload") == 1
    assert RESULT_QUERIES.with_log.count("log_payload") == 1


def test_all_sql_avoids_unsafe_or_non_v01_constructs() -> None:
    """Forbid broad projections, merge-dependent reads and cluster DDL syntax."""
    all_queries = (
        RESULT_QUERIES.allocator,
        PROGRESS_QUERIES.allocator,
        RESULT_QUERIES.readiness,
        RESULT_QUERIES.no_log,
        RESULT_QUERIES.with_log,
        PROGRESS_QUERIES.latest,
        RESULT_QUERIES.confirmation,
        PROGRESS_QUERIES.confirmation,
    )

    for query in all_queries:
        normalized = query.upper()
        assert "SELECT *" not in normalized
        assert " FINAL" not in normalized
        assert "ON CLUSTER" not in normalized
        assert "ARGMAX" not in normalized


def test_byte_format_maps_are_exact_and_immutable() -> None:
    """Request opaque ClickHouse Strings as bytes under their projected names."""
    assert isinstance(NO_LOG_COLUMN_FORMATS, MappingProxyType)
    assert isinstance(WITH_LOG_COLUMN_FORMATS, MappingProxyType)
    assert isinstance(PROGRESS_COLUMN_FORMATS, MappingProxyType)
    assert dict(NO_LOG_COLUMN_FORMATS) == {"result_payload": "bytes"}
    assert dict(WITH_LOG_COLUMN_FORMATS) == {
        "result_payload": "bytes",
        "log_payload": "bytes",
    }
    assert dict(PROGRESS_COLUMN_FORMATS) == {"progress_payload": "bytes"}
    with pytest.raises(TypeError, match="does not support item assignment"):
        cast("Any", NO_LOG_COLUMN_FORMATS)["result_payload"] = "string"


def test_insert_contracts_reuse_migration_v1_source_of_truth() -> None:
    """Keep native insert names/types identical to physical migration order."""
    assert RESULT_INSERT_COLUMN_NAMES is RESULT_COLUMN_NAMES
    assert RESULT_INSERT_COLUMN_TYPES is RESULT_COLUMN_TYPES
    assert PROGRESS_INSERT_COLUMN_NAMES is PROGRESS_COLUMN_NAMES
    assert PROGRESS_INSERT_COLUMN_TYPES is PROGRESS_COLUMN_TYPES


def test_point_parameters_preserve_values_without_sql_interpolation() -> None:
    """Bind even SQL-looking task ids as values under typed placeholders."""
    namespace = "tenant:blue"
    task_id = "task' OR 1 = 1 --"

    assert point_parameters(namespace, task_id) == {
        "namespace": namespace,
        "task_id": task_id,
    }
    assert namespace not in RESULT_QUERIES.no_log
    assert task_id not in RESULT_QUERIES.no_log


def test_query_sets_bind_their_own_table_without_mutating_values() -> None:
    """Keep identical SQL reusable while preserving each physical table identity."""
    values: dict[str, object] = {"namespace": "tenant", "task_id": "task"}

    result_parameters = RESULT_QUERIES.bind(values)
    progress_parameters = PROGRESS_QUERIES.bind(values)
    values["task_id"] = "changed"

    assert result_parameters == {
        "database": "analytics",
        "table": "task_results",
        "namespace": "tenant",
        "task_id": "task",
    }
    assert progress_parameters == {
        "database": "analytics",
        "table": "task_progress",
        "namespace": "tenant",
        "task_id": "task",
    }


@pytest.mark.parametrize(("namespace", "task_id"), [(cast("Any", 1), "task"), ("tenant", cast("Any", 1))])
def test_point_parameters_reject_non_string_values(namespace: str, task_id: str) -> None:
    """Fail before I/O rather than relying on driver coercion."""
    with pytest.raises(TypeError, match="must be a string"):
        point_parameters(namespace, task_id)


def test_result_confirmation_parameters_cover_generation_and_state() -> None:
    """Bind every field in the frozen result/tombstone logical identity."""
    record = _result_record()

    assert result_confirmation_parameters(record) == {
        "namespace": record.namespace,
        "task_id": record.task_id,
        "generation_at": record.generation_at,
        "generation_id": record.generation_id,
        "state": record.state,
        "written_at": record.written_at,
        "visible_until": record.visible_until,
        "purge_at": record.purge_at,
    }


def test_result_confirmation_distinguishes_concurrent_tombstone_attempts() -> None:
    """Do not let one same-generation tombstone acknowledge another deadline."""
    first = _result_record()
    second = ResultRecord(
        namespace=first.namespace,
        task_id=first.task_id,
        generation_at=first.generation_at,
        generation_id=first.generation_id,
        state=first.state,
        written_at=first.written_at + timedelta(microseconds=1),
        visible_until=first.visible_until,
        purge_at=first.purge_at + timedelta(microseconds=1),
        result_payload=first.result_payload,
        log_payload=first.log_payload,
    )

    assert result_confirmation_parameters(first) != result_confirmation_parameters(second)


def test_progress_confirmation_parameters_cover_complete_identity() -> None:
    """Bind every field in the frozen progress logical identity."""
    record = _progress_record()

    assert progress_confirmation_parameters(record) == {
        "namespace": record.namespace,
        "task_id": record.task_id,
        "generation_at": record.generation_at,
        "generation_id": record.generation_id,
    }


def test_confirmation_parameter_helpers_reject_wrong_record_classes() -> None:
    """Prevent a result identity from being confirmed through the progress path."""
    with pytest.raises(TypeError, match="ResultRecord"):
        result_confirmation_parameters(_progress_record())
    with pytest.raises(TypeError, match="ProgressRecord"):
        progress_confirmation_parameters(_result_record())
