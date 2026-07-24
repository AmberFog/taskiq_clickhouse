"""Executable storage-schema and latest-query hypotheses for TASK-017."""

import asyncio
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Final
from uuid import UUID

from clickhouse_connect.driver.asyncclient import AsyncClient
from clickhouse_connect.driver.exceptions import ClickHouseError


RESULT_TABLE: Final = "poc_taskiq_results"
PROGRESS_TABLE: Final = "poc_taskiq_progress"
NAMESPACE: Final = "storage-poc"
DDL_SETTINGS: Final = {"wait_end_of_query": 1}
INSERT_SETTINGS: Final = {
    "async_insert": 0,
    "wait_for_async_insert": 1,
    "wait_end_of_query": 1,
}
ACKNOWLEDGED_ASYNC_SETTINGS: Final = {
    "async_insert": 1,
    "wait_for_async_insert": 1,
    "wait_end_of_query": 1,
}
QUERY_TIMEOUT_SECONDS: Final = 15
WIDE_PART_MARGIN_BYTES: Final = 1_000_000
MAX_WIDE_PART_PROBE_BYTES: Final = 32_000_000
EXPLAIN_EDGE_ROW_COUNT: Final = 8_192
EXPLAIN_TARGET_ROW_COUNT: Final = 24_576
EXPLAIN_ROW_COUNT: Final = 2 * EXPLAIN_EDGE_ROW_COUNT + EXPLAIN_TARGET_ROW_COUNT
STATE_RESULT: Final = 0
STATE_TOMBSTONE: Final = 1
RESULT_PAYLOAD: Final = b"\x00result-payload\xff"
LOG_PAYLOAD: Final = b"\x00log-payload\xfe"
PARTITION_NEWER_GENERATION_ID: Final = UUID("00000000-0000-4000-8000-000000000002")
TARGET_NEWER_GENERATION_ID: Final = UUID("00000000-0000-4000-8000-00000000000b")
PROGRESS_NEWER_GENERATION_ID: Final = UUID("00000000-0000-4000-8000-000000000042")

RESULT_COLUMN_TYPES: Final = (
    ("namespace", "String"),
    ("task_id", "String"),
    ("generation_at", "DateTime64(6, 'UTC')"),
    ("generation_id", "UUID"),
    ("state", "UInt8"),
    ("written_at", "DateTime64(6, 'UTC')"),
    ("visible_until", "DateTime64(6, 'UTC')"),
    ("purge_at", "DateTime64(6, 'UTC')"),
    ("result_payload", "String"),
    ("log_payload", "String"),
)
PROGRESS_COLUMN_TYPES: Final = (
    ("namespace", "String"),
    ("task_id", "String"),
    ("generation_at", "DateTime64(6, 'UTC')"),
    ("generation_id", "UUID"),
    ("written_at", "DateTime64(6, 'UTC')"),
    ("visible_until", "DateTime64(6, 'UTC')"),
    ("purge_at", "DateTime64(6, 'UTC')"),
    ("progress_payload", "String"),
)

DROP_RESULT_TABLE_QUERY: Final = "DROP TABLE IF EXISTS poc_taskiq_results SYNC"
DROP_PROGRESS_TABLE_QUERY: Final = "DROP TABLE IF EXISTS poc_taskiq_progress SYNC"
CREATE_RESULT_TABLE_QUERY: Final = """
CREATE TABLE poc_taskiq_results
(
    namespace String,
    task_id String,
    generation_at DateTime64(6, 'UTC'),
    generation_id UUID,
    state UInt8,
    written_at DateTime64(6, 'UTC'),
    visible_until DateTime64(6, 'UTC'),
    purge_at DateTime64(6, 'UTC'),
    result_payload String,
    log_payload String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(purge_at)
PRIMARY KEY (namespace, task_id)
ORDER BY (namespace, task_id, generation_at, generation_id, state)
TTL purge_at DELETE
"""
CREATE_PROGRESS_TABLE_QUERY: Final = """
CREATE TABLE poc_taskiq_progress
(
    namespace String,
    task_id String,
    generation_at DateTime64(6, 'UTC'),
    generation_id UUID,
    written_at DateTime64(6, 'UTC'),
    visible_until DateTime64(6, 'UTC'),
    purge_at DateTime64(6, 'UTC'),
    progress_payload String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(purge_at)
PRIMARY KEY (namespace, task_id)
ORDER BY (namespace, task_id, generation_at, generation_id)
TTL purge_at DELETE
"""
SYSTEM_TABLE_QUERY: Final = """
SELECT engine, engine_full, partition_key, sorting_key, primary_key,
       sampling_key, create_table_query
FROM system.tables
WHERE database = {database:String} AND name = {table:String}
"""
SYSTEM_COLUMNS_QUERY: Final = """
SELECT position, name, type, default_kind, default_expression,
       compression_codec, is_in_partition_key, is_in_sorting_key,
       is_in_primary_key, is_in_sampling_key
FROM system.columns
WHERE database = {database:String} AND table = {table:String}
ORDER BY position
"""
DESCRIBE_TABLE_QUERY: Final = "DESCRIBE TABLE {table:Identifier}"
SERVER_NOW_QUERY: Final = "SELECT now64(6, 'UTC')"
# The full sorting-key order is intentional: ClickHouse 25.8 does not elide the
# fixed namespace/task_id prefix when choosing its read-in-order algorithm.
LATEST_RESULT_QUERY: Final = """
SELECT generation_at, generation_id, state, visible_until, purge_at,
       result_payload
FROM poc_taskiq_results
PREWHERE namespace = {namespace:String} AND task_id = {task_id:String}
ORDER BY namespace DESC, task_id DESC, generation_at DESC, generation_id DESC, state DESC
LIMIT 1
"""
LATEST_IDENTITY_QUERY: Final = """
SELECT generation_id, state
FROM poc_taskiq_results
PREWHERE namespace = {namespace:String} AND task_id = {task_id:String}
ORDER BY namespace DESC, task_id DESC, generation_at DESC, generation_id DESC, state DESC
LIMIT 1
"""
LATEST_PROGRESS_QUERY: Final = """
SELECT generation_at, generation_id, visible_until, purge_at, progress_payload
FROM poc_taskiq_progress
PREWHERE namespace = {namespace:String} AND task_id = {task_id:String}
ORDER BY namespace DESC, task_id DESC, generation_at DESC, generation_id DESC
LIMIT 1
"""
GENERATION_STATE_QUERY: Final = """
SELECT state
FROM poc_taskiq_results
PREWHERE namespace = {namespace:String} AND task_id = {task_id:String}
WHERE generation_at = {generation_at:DateTime64(6, 'UTC')}
  AND generation_id = {generation_id:UUID}
ORDER BY namespace DESC, task_id DESC, generation_at DESC, generation_id DESC, state DESC
LIMIT 1
"""
LATEST_VISIBILITY_QUERY: Final = """
SELECT result_payload, visible_until, now64(6, 'UTC') AS observed_at
FROM poc_taskiq_results
PREWHERE namespace = {namespace:String} AND task_id = {task_id:String}
ORDER BY namespace DESC, task_id DESC, generation_at DESC, generation_id DESC, state DESC
LIMIT 1
"""
PREFILTER_VISIBILITY_QUERY: Final = """
SELECT result_payload
FROM poc_taskiq_results
PREWHERE namespace = {namespace:String} AND task_id = {task_id:String}
WHERE state = 0 AND visible_until > now64(6, 'UTC')
ORDER BY namespace DESC, task_id DESC, generation_at DESC, generation_id DESC, state DESC
LIMIT 1
"""
DIRECT_NO_LOG_QUERY: Final = """
SELECT result_payload
FROM poc_taskiq_results
PREWHERE namespace = {namespace:String} AND task_id = {task_id:String}
ORDER BY namespace DESC, task_id DESC, generation_at DESC, generation_id DESC, state DESC
LIMIT 1
"""
ALIASED_NO_LOG_QUERY: Final = """
SELECT result_payload AS payload_bytes
FROM poc_taskiq_results
PREWHERE namespace = {namespace:String} AND task_id = {task_id:String}
ORDER BY namespace DESC, task_id DESC, generation_at DESC, generation_id DESC, state DESC
LIMIT 1
"""
WITH_LOG_QUERY: Final = """
SELECT result_payload, log_payload
FROM poc_taskiq_results
PREWHERE namespace = {namespace:String} AND task_id = {task_id:String}
ORDER BY namespace DESC, task_id DESC, generation_at DESC, generation_id DESC, state DESC
LIMIT 1
"""
EXPLAIN_LATEST_QUERY: Final = """
EXPLAIN indexes = 1
SELECT generation_at, generation_id, state
FROM poc_taskiq_results
PREWHERE namespace = {namespace:String} AND task_id = {task_id:String}
ORDER BY namespace DESC, task_id DESC, generation_at DESC, generation_id DESC, state DESC
LIMIT 1
SETTINGS optimize_read_in_order = 1
"""
EXPLAIN_LATEST_PIPELINE_QUERY: Final = """
EXPLAIN PIPELINE
SELECT generation_at, generation_id, state
FROM poc_taskiq_results
PREWHERE namespace = {namespace:String} AND task_id = {task_id:String}
ORDER BY namespace DESC, task_id DESC, generation_at DESC, generation_id DESC, state DESC
LIMIT 1
SETTINGS optimize_read_in_order = 1
"""
EXPLAIN_LATEST_PROGRESS_QUERY: Final = """
EXPLAIN indexes = 1
SELECT generation_at, generation_id
FROM poc_taskiq_progress
PREWHERE namespace = {namespace:String} AND task_id = {task_id:String}
ORDER BY namespace DESC, task_id DESC, generation_at DESC, generation_id DESC
LIMIT 1
SETTINGS optimize_read_in_order = 1
"""
EXPLAIN_LATEST_PROGRESS_PIPELINE_QUERY: Final = """
EXPLAIN PIPELINE
SELECT generation_at, generation_id
FROM poc_taskiq_progress
PREWHERE namespace = {namespace:String} AND task_id = {task_id:String}
ORDER BY namespace DESC, task_id DESC, generation_at DESC, generation_id DESC
LIMIT 1
SETTINGS optimize_read_in_order = 1
"""
ACTIVE_PARTS_QUERY: Final = """
SELECT partition, part_type, rows, bytes_on_disk
FROM system.parts
WHERE database = {database:String} AND table = {table:String} AND active
ORDER BY partition, name
"""
WIDE_PART_THRESHOLD_QUERY: Final = """
SELECT toUInt64(value)
FROM system.merge_tree_settings
WHERE name = 'min_bytes_for_wide_part'
"""


@dataclass(frozen=True, slots=True)
class CandidateTables:
    """Names of disposable schema candidates in one worker database."""

    database: str
    result: str = RESULT_TABLE
    progress: str = PROGRESS_TABLE


@dataclass(frozen=True, slots=True)
class SystemColumn:
    """Normalized system.columns row."""

    position: int
    name: str
    type_name: str
    default_kind: str
    default_expression: str
    compression_codec: str
    in_partition_key: bool
    in_sorting_key: bool
    in_primary_key: bool
    in_sampling_key: bool


@dataclass(frozen=True, slots=True)
class DescribedColumn:
    """Normalized DESCRIBE TABLE row."""

    name: str
    type_name: str
    default_type: str
    default_expression: str
    comment: str
    codec_expression: str
    ttl_expression: str


@dataclass(frozen=True, slots=True)
class TableMetadata:
    """Normalized table, column and DESCRIBE evidence."""

    engine: str
    engine_full: str
    partition_key: str
    sorting_key: str
    primary_key: str
    sampling_key: str
    create_table_query: str
    columns: tuple[SystemColumn, ...]
    described_columns: tuple[DescribedColumn, ...]


@dataclass(frozen=True, slots=True)
class SchemaMetadata:
    """Physical observations for both candidate tables."""

    result: TableMetadata
    progress: TableMetadata


@dataclass(frozen=True, slots=True)
class LatestPartitionObservation:
    """Latest payload selected independently of purge partition order."""

    selected_payload: bytes
    selected_generation_id: UUID
    partitions: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class AcknowledgedVisibilityObservation:
    """Immediate latest-read state after result and targeted tombstone writes."""

    synchronous_payload: bytes
    acknowledged_tombstone_state: int
    acknowledged_generation_id: UUID


@dataclass(frozen=True, slots=True)
class StateTieObservation:
    """Targeted tombstone ordering without hiding a newer generation."""

    targeted_state: int
    latest_generation_id: UUID
    latest_state: int
    partitions: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class VisibilityObservation:
    """Difference between correct latest-first and unsafe pre-filtering."""

    latest_payload: bytes
    latest_visible: bool
    prefiltered_payload: bytes


@dataclass(frozen=True, slots=True)
class ProjectionObservation:
    """Exact bytes and columns crossing the client/network boundary."""

    direct_columns: tuple[str, ...]
    direct_payload: bytes
    aliased_columns: tuple[str, ...]
    aliased_payload: bytes


@dataclass(frozen=True, slots=True)
class ExplainObservation:
    """Normalized EXPLAIN text and derived pruning/read-order facts."""

    text: str
    uses_primary_key: bool
    fixes_namespace_and_task: bool
    reads_in_reverse_order: bool


@dataclass(frozen=True, slots=True)
class PartObservation:
    """Part layout and the deliberately limited projection conclusion."""

    part_types: tuple[str, ...]
    partitions: tuple[str, ...]
    no_log_columns: tuple[str, ...]
    with_log_columns: tuple[str, ...]
    no_log_payload: bytes
    physical_io_proven_by_projection: bool = False


@dataclass(frozen=True, slots=True)
class _ResultRow:
    task_id: str
    generation_at: datetime
    generation_id: UUID
    purge_at: datetime
    result_payload: bytes
    state: int = STATE_RESULT
    visible_until: datetime | None = None
    log_payload: bytes = b""

    def as_tuple(self) -> tuple[object, ...]:
        visible_until = self.visible_until or self.purge_at - timedelta(days=1)
        return (
            NAMESPACE,
            self.task_id,
            self.generation_at,
            self.generation_id,
            self.state,
            self.generation_at,
            visible_until,
            self.purge_at,
            self.result_payload,
            self.log_payload,
        )


@dataclass(frozen=True, slots=True)
class _ProgressRow:
    task_id: str
    generation_at: datetime
    generation_id: UUID
    purge_at: datetime
    progress_payload: bytes
    visible_until: datetime | None = None

    def as_tuple(self) -> tuple[object, ...]:
        visible_until = self.visible_until or self.purge_at - timedelta(days=1)
        return (
            NAMESPACE,
            self.task_id,
            self.generation_at,
            self.generation_id,
            self.generation_at,
            visible_until,
            self.purge_at,
            self.progress_payload,
        )


@asynccontextmanager
async def candidate_tables(client: AsyncClient, database: str) -> AsyncIterator[CandidateTables]:
    """Create and always remove the disposable v1 table candidates."""
    await _drop_candidate_tables(client)
    try:
        await client.command(CREATE_RESULT_TABLE_QUERY, settings=DDL_SETTINGS)
        await client.command(CREATE_PROGRESS_TABLE_QUERY, settings=DDL_SETTINGS)
    except BaseException:
        await _drop_candidate_tables(client)
        raise
    try:
        yield CandidateTables(database=database)
    finally:
        await _drop_candidate_tables(client)


async def inspect_candidate_schema(client: AsyncClient, tables: CandidateTables) -> SchemaMetadata:
    """Read normalized system catalog and DESCRIBE evidence."""
    async with asyncio.timeout(QUERY_TIMEOUT_SECONDS):
        result = await _inspect_table(client, tables.database, tables.result)
        progress = await _inspect_table(client, tables.database, tables.progress)
    return SchemaMetadata(result=result, progress=progress)


async def observe_latest_across_partitions(client: AsyncClient, tables: CandidateTables) -> LatestPartitionObservation:
    """Prove generation ordering wins across reversed purge partitions."""
    now = await _server_now(client)
    older_id = UUID("00000000-0000-4000-8000-000000000001")
    newer_id = PARTITION_NEWER_GENERATION_ID
    task_id = "partition-order"
    rows = (
        _ResultRow(
            task_id=task_id,
            generation_at=now - timedelta(minutes=2),
            generation_id=older_id,
            purge_at=datetime(2090, 2, 1, tzinfo=now.tzinfo),
            result_payload=b"older",
        ),
        _ResultRow(
            task_id=task_id,
            generation_at=now - timedelta(minutes=1),
            generation_id=newer_id,
            purge_at=datetime(2090, 1, 1, tzinfo=now.tzinfo),
            result_payload=b"newer",
        ),
    )
    await _insert_results(client, rows)
    query_result = await client.query(
        LATEST_RESULT_QUERY,
        parameters={"namespace": NAMESPACE, "task_id": task_id},
        column_formats={"result_payload": "bytes"},
    )
    selected = query_result.result_rows[0]
    parts = await _active_parts(client, tables.database)
    return LatestPartitionObservation(
        selected_payload=_require_bytes(selected[5]),
        selected_generation_id=_require_uuid(selected[1]),
        partitions=tuple(str(row[0]) for row in parts),
    )


async def observe_latest_progress_across_partitions(
    client: AsyncClient,
    tables: CandidateTables,
) -> LatestPartitionObservation:
    """Prove progress generation ordering across reversed purge partitions."""
    now = await _server_now(client)
    task_id = "progress-partition-order"
    rows = (
        _ProgressRow(
            task_id=task_id,
            generation_at=now - timedelta(minutes=2),
            generation_id=UUID("00000000-0000-4000-8000-000000000041"),
            purge_at=datetime(2090, 8, 1, tzinfo=now.tzinfo),
            progress_payload=b"older-progress",
        ),
        _ProgressRow(
            task_id=task_id,
            generation_at=now - timedelta(minutes=1),
            generation_id=PROGRESS_NEWER_GENERATION_ID,
            purge_at=datetime(2090, 7, 1, tzinfo=now.tzinfo),
            progress_payload=b"newer-progress",
        ),
    )
    await _insert_progress(client, rows)
    selected = (
        await client.query(
            LATEST_PROGRESS_QUERY,
            parameters={"namespace": NAMESPACE, "task_id": task_id},
            column_formats={"progress_payload": "bytes"},
        )
    ).result_rows[0]
    parts = await _active_parts(client, tables.database, PROGRESS_TABLE)
    return LatestPartitionObservation(
        selected_payload=_require_bytes(selected[4]),
        selected_generation_id=_require_uuid(selected[1]),
        partitions=tuple(str(row[0]) for row in parts),
    )


async def observe_acknowledged_result_and_tombstone_visibility(
    client: AsyncClient,
) -> AcknowledgedVisibilityObservation:
    """Read a sync result and acknowledged async tombstone through latest SQL."""
    now = await _server_now(client)
    task_id = "acknowledged-latest"
    generation_id = UUID("00000000-0000-4000-8000-000000000051")
    result = _ResultRow(
        task_id=task_id,
        generation_at=now,
        generation_id=generation_id,
        purge_at=now + timedelta(days=90),
        result_payload=b"visible-result",
    )
    parameters: dict[str, object] = {"namespace": NAMESPACE, "task_id": task_id}
    await _insert_results(client, (result,))
    synchronous_row = (
        await client.query(
            LATEST_RESULT_QUERY,
            parameters=parameters,
            column_formats={"result_payload": "bytes"},
        )
    ).result_rows[0]
    tombstone = _ResultRow(
        task_id=task_id,
        generation_at=now,
        generation_id=generation_id,
        purge_at=now + timedelta(days=90),
        result_payload=b"",
        state=STATE_TOMBSTONE,
    )
    await _insert_results(client, (tombstone,), settings=ACKNOWLEDGED_ASYNC_SETTINGS)
    tombstone_row = (await client.query(LATEST_IDENTITY_QUERY, parameters=parameters)).result_rows[0]
    return AcknowledgedVisibilityObservation(
        synchronous_payload=_require_bytes(synchronous_row[5]),
        acknowledged_tombstone_state=int(tombstone_row[1]),
        acknowledged_generation_id=_require_uuid(tombstone_row[0]),
    )


async def observe_targeted_state_tie(
    client: AsyncClient,
    tables: CandidateTables,
) -> StateTieObservation:
    """Prove a tombstone wins only its exact generation."""
    now = await _server_now(client)
    generation_a = UUID("00000000-0000-4000-8000-00000000000a")
    generation_b = TARGET_NEWER_GENERATION_ID
    task_id = "targeted-state"
    generation_a_at = now - timedelta(minutes=2)
    rows = (
        _ResultRow(
            task_id=task_id,
            generation_at=generation_a_at,
            generation_id=generation_a,
            purge_at=datetime(2090, 6, 1, tzinfo=now.tzinfo),
            result_payload=b"result-a",
        ),
        _ResultRow(
            task_id=task_id,
            generation_at=now - timedelta(minutes=1),
            generation_id=generation_b,
            purge_at=datetime(2090, 4, 1, tzinfo=now.tzinfo),
            result_payload=b"result-b",
        ),
        _ResultRow(
            task_id=task_id,
            generation_at=generation_a_at,
            generation_id=generation_a,
            purge_at=datetime(2090, 5, 1, tzinfo=now.tzinfo),
            result_payload=b"",
            state=STATE_TOMBSTONE,
        ),
    )
    await _insert_results(client, rows)
    targeted_row = (
        await client.query(
            GENERATION_STATE_QUERY,
            parameters={
                "namespace": NAMESPACE,
                "task_id": task_id,
                "generation_at": generation_a_at,
                "generation_id": generation_a,
            },
        )
    ).result_rows[0]
    latest_row = (
        await client.query(
            LATEST_IDENTITY_QUERY,
            parameters={"namespace": NAMESPACE, "task_id": task_id},
        )
    ).result_rows[0]
    parts = await _active_parts(client, tables.database, RESULT_TABLE)
    return StateTieObservation(
        targeted_state=int(targeted_row[0]),
        latest_generation_id=_require_uuid(latest_row[0]),
        latest_state=int(latest_row[1]),
        partitions=tuple(str(row[0]) for row in parts),
    )


async def observe_latest_before_visibility(client: AsyncClient) -> VisibilityObservation:
    """Prove visibility filtering after latest selection prevents resurrection."""
    now = await _server_now(client)
    task_id = "latest-before-visibility"
    rows = (
        _ResultRow(
            task_id=task_id,
            generation_at=now - timedelta(minutes=2),
            generation_id=UUID("00000000-0000-4000-8000-000000000011"),
            purge_at=now + timedelta(days=90),
            result_payload=b"older-visible",
            visible_until=now + timedelta(days=1),
        ),
        _ResultRow(
            task_id=task_id,
            generation_at=now - timedelta(minutes=1),
            generation_id=UUID("00000000-0000-4000-8000-000000000012"),
            purge_at=now + timedelta(days=90),
            result_payload=b"newer-expired",
            visible_until=now - timedelta(minutes=1),
        ),
    )
    await _insert_results(client, rows)
    parameters = {"namespace": NAMESPACE, "task_id": task_id}
    latest_row = (
        await client.query(
            LATEST_VISIBILITY_QUERY,
            parameters=parameters,
            column_formats={"result_payload": "bytes"},
        )
    ).result_rows[0]
    prefiltered_row = (
        await client.query(
            PREFILTER_VISIBILITY_QUERY,
            parameters=parameters,
            column_formats={"result_payload": "bytes"},
        )
    ).result_rows[0]
    visible_until = _require_datetime(latest_row[1])
    observed_at = _require_datetime(latest_row[2])
    return VisibilityObservation(
        latest_payload=_require_bytes(latest_row[0]),
        latest_visible=visible_until > observed_at,
        prefiltered_payload=_require_bytes(prefiltered_row[0]),
    )


async def observe_no_log_projections(client: AsyncClient) -> ProjectionObservation:
    """Prove direct and aliased no-log projections preserve opaque bytes."""
    now = await _server_now(client)
    task_id = "no-log-projection"
    await _insert_results(
        client,
        (
            _ResultRow(
                task_id=task_id,
                generation_at=now,
                generation_id=UUID("00000000-0000-4000-8000-000000000021"),
                purge_at=now + timedelta(days=90),
                result_payload=RESULT_PAYLOAD,
                log_payload=LOG_PAYLOAD,
            ),
        ),
    )
    parameters = {"namespace": NAMESPACE, "task_id": task_id}
    direct = await client.query(
        DIRECT_NO_LOG_QUERY,
        parameters=parameters,
        column_formats={"result_payload": "bytes"},
    )
    aliased = await client.query(
        ALIASED_NO_LOG_QUERY,
        parameters=parameters,
        column_formats={"payload_bytes": "bytes"},
    )
    return ProjectionObservation(
        direct_columns=tuple(direct.column_names),
        direct_payload=_require_bytes(direct.result_rows[0][0]),
        aliased_columns=tuple(aliased.column_names),
        aliased_payload=_require_bytes(aliased.result_rows[0][0]),
    )


async def observe_explain_pruning(client: AsyncClient) -> ExplainObservation:
    """Collect primary-key pruning and reverse read-order evidence."""
    now = await _server_now(client)
    task_id = "explain-target"
    await _insert_results(
        client,
        tuple(
            _ResultRow(
                task_id=_explain_task_id(row_number, task_id),
                generation_at=now + timedelta(microseconds=row_number),
                generation_id=UUID(int=row_number + 100),
                purge_at=now + timedelta(days=90),
                result_payload=b"x",
            )
            for row_number in range(EXPLAIN_ROW_COUNT)
        ),
    )
    parameters: dict[str, object] = {"namespace": NAMESPACE, "task_id": task_id}
    return await _collect_explain(
        client,
        EXPLAIN_LATEST_QUERY,
        EXPLAIN_LATEST_PIPELINE_QUERY,
        parameters,
    )


async def observe_progress_explain_pruning(client: AsyncClient) -> ExplainObservation:
    """Collect progress primary-key pruning and reverse read-order evidence."""
    now = await _server_now(client)
    task_id = "progress-explain-target"
    await _insert_progress(
        client,
        tuple(
            _ProgressRow(
                task_id=_explain_task_id(row_number, task_id),
                generation_at=now + timedelta(microseconds=row_number),
                generation_id=UUID(int=row_number + EXPLAIN_ROW_COUNT),
                purge_at=now + timedelta(days=90),
                progress_payload=b"x",
            )
            for row_number in range(EXPLAIN_ROW_COUNT)
        ),
    )
    return await _collect_explain(
        client,
        EXPLAIN_LATEST_PROGRESS_QUERY,
        EXPLAIN_LATEST_PROGRESS_PIPELINE_QUERY,
        {"namespace": NAMESPACE, "task_id": task_id},
    )


async def _collect_explain(
    client: AsyncClient,
    index_query: str,
    pipeline_query: str,
    parameters: dict[str, object],
) -> ExplainObservation:
    index_rows = (await client.query(index_query, parameters=parameters)).result_rows
    pipeline_rows = (await client.query(pipeline_query, parameters=parameters)).result_rows
    index_text = "\n".join(str(row[0]) for row in index_rows)
    pipeline_text = "\n".join(str(row[0]) for row in pipeline_rows)
    text = f"indexes:\n{index_text}\npipeline:\n{pipeline_text}"
    normalized_index = index_text.casefold()
    normalized_pipeline = pipeline_text.casefold()
    return ExplainObservation(
        text=text,
        uses_primary_key="primarykey" in normalized_index,
        fixes_namespace_and_task="namespace" in normalized_index and "task_id" in normalized_index,
        reads_in_reverse_order="inreverseorder" in normalized_pipeline,
    )


async def observe_part_projection_boundary(client: AsyncClient, tables: CandidateTables) -> PartObservation:
    """Create Compact/Wide parts without inferring disk I/O from projections."""
    threshold_row = (await client.query(WIDE_PART_THRESHOLD_QUERY)).result_rows[0]
    threshold = int(threshold_row[0])
    wide_payload_size = threshold + WIDE_PART_MARGIN_BYTES
    if not WIDE_PART_MARGIN_BYTES < wide_payload_size <= MAX_WIDE_PART_PROBE_BYTES:
        msg = f"unsupported min_bytes_for_wide_part for bounded POC: {threshold}"
        raise ValueError(msg)
    now = await _server_now(client)
    small_task = "compact-part"
    wide_task = "wide-part"
    rows = (
        _ResultRow(
            task_id=small_task,
            generation_at=now,
            generation_id=UUID("00000000-0000-4000-8000-000000000031"),
            purge_at=datetime(2090, 3, 1, tzinfo=now.tzinfo),
            result_payload=b"small",
            log_payload=b"small-log",
        ),
        _ResultRow(
            task_id=wide_task,
            generation_at=now + timedelta(microseconds=1),
            generation_id=UUID("00000000-0000-4000-8000-000000000032"),
            purge_at=datetime(2090, 4, 1, tzinfo=now.tzinfo),
            result_payload=b"wide-result",
            log_payload=b"x" * wide_payload_size,
        ),
    )
    for row in rows:
        await _insert_results(client, (row,))
    parts = await _active_parts(client, tables.database)
    parameters = {"namespace": NAMESPACE, "task_id": wide_task}
    no_log = await client.query(
        DIRECT_NO_LOG_QUERY,
        parameters=parameters,
        column_formats={"result_payload": "bytes"},
    )
    with_log = await client.query(
        WITH_LOG_QUERY,
        parameters=parameters,
        column_formats={"result_payload": "bytes", "log_payload": "bytes"},
    )
    return PartObservation(
        part_types=tuple(str(row[1]) for row in parts),
        partitions=tuple(str(row[0]) for row in parts),
        no_log_columns=tuple(no_log.column_names),
        with_log_columns=tuple(with_log.column_names),
        no_log_payload=_require_bytes(no_log.result_rows[0][0]),
    )


async def _inspect_table(client: AsyncClient, database: str, table: str) -> TableMetadata:
    parameters = {"database": database, "table": table}
    table_row = (await client.query(SYSTEM_TABLE_QUERY, parameters=parameters)).result_rows[0]
    column_rows = (await client.query(SYSTEM_COLUMNS_QUERY, parameters=parameters)).result_rows
    describe = await client.query(DESCRIBE_TABLE_QUERY, parameters={"table": table})
    return TableMetadata(
        engine=str(table_row[0]),
        engine_full=_normalize_expression(table_row[1]),
        partition_key=_normalize_expression(table_row[2]),
        sorting_key=_normalize_expression(table_row[3]),
        primary_key=_normalize_expression(table_row[4]),
        sampling_key=_normalize_expression(table_row[5]),
        create_table_query=_normalize_expression(table_row[6]),
        columns=tuple(_system_column(row) for row in column_rows),
        described_columns=tuple(_described_column(row) for row in describe.result_rows),
    )


def _system_column(row: Sequence[object]) -> SystemColumn:
    return SystemColumn(
        position=_require_int(row[0]),
        name=str(row[1]),
        type_name=str(row[2]),
        default_kind=str(row[3]),
        default_expression=str(row[4]),
        compression_codec=str(row[5]),
        in_partition_key=_require_flag(row[6]),
        in_sorting_key=_require_flag(row[7]),
        in_primary_key=_require_flag(row[8]),
        in_sampling_key=_require_flag(row[9]),
    )


def _described_column(row: Sequence[object]) -> DescribedColumn:
    return DescribedColumn(
        name=str(row[0]),
        type_name=str(row[1]),
        default_type=str(row[2]),
        default_expression=str(row[3]),
        comment=str(row[4]),
        codec_expression=str(row[5]),
        ttl_expression=str(row[6]),
    )


async def _insert_results(
    client: AsyncClient,
    rows: Sequence[_ResultRow],
    *,
    settings: Mapping[str, int] = INSERT_SETTINGS,
) -> None:
    await client.insert(
        table=RESULT_TABLE,
        data=tuple(row.as_tuple() for row in rows),
        column_names=tuple(name for name, _type_name in RESULT_COLUMN_TYPES),
        column_type_names=tuple(type_name for _name, type_name in RESULT_COLUMN_TYPES),
        settings=dict(settings),
    )


async def _insert_progress(client: AsyncClient, rows: Sequence[_ProgressRow]) -> None:
    await client.insert(
        table=PROGRESS_TABLE,
        data=tuple(row.as_tuple() for row in rows),
        column_names=tuple(name for name, _type_name in PROGRESS_COLUMN_TYPES),
        column_type_names=tuple(type_name for _name, type_name in PROGRESS_COLUMN_TYPES),
        settings=INSERT_SETTINGS,
    )


async def _server_now(client: AsyncClient) -> datetime:
    value = (await client.query(SERVER_NOW_QUERY)).result_rows[0][0]
    return _require_datetime(value)


async def _active_parts(
    client: AsyncClient,
    database: str,
    table: str = RESULT_TABLE,
) -> Sequence[Sequence[object]]:
    return (
        await client.query(
            ACTIVE_PARTS_QUERY,
            parameters={"database": database, "table": table},
        )
    ).result_rows


async def _drop_candidate_tables(client: AsyncClient) -> None:
    failures = 0
    try:
        await client.command(DROP_RESULT_TABLE_QUERY, settings=DDL_SETTINGS)
    except ClickHouseError:
        failures += 1
    try:
        await client.command(DROP_PROGRESS_TABLE_QUERY, settings=DDL_SETTINGS)
    except ClickHouseError:
        failures += 1
    if failures:
        msg = f"failed to drop {failures} candidate table(s)"
        raise RuntimeError(msg)


def _normalize_expression(value: object) -> str:
    return " ".join(str(value).split())


def _require_bytes(value: object) -> bytes:
    if not isinstance(value, bytes):
        msg = f"expected bytes, got {type(value).__name__}"
        raise TypeError(msg)
    return value


def _require_datetime(value: object) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        msg = "expected an aware datetime"
        raise TypeError(msg)
    return value


def _require_uuid(value: object) -> UUID:
    if not isinstance(value, UUID):
        msg = f"expected UUID, got {type(value).__name__}"
        raise TypeError(msg)
    return value


def _require_int(value: object) -> int:
    if not isinstance(value, int):
        msg = f"expected int, got {type(value).__name__}"
        raise TypeError(msg)
    return value


def _require_flag(value: object) -> bool:
    integer = _require_int(value)
    if integer not in (0, 1):
        msg = f"expected UInt8 flag, got {integer}"
        raise ValueError(msg)
    return bool(integer)


def _explain_task_id(row_number: int, target: str) -> str:
    if row_number < EXPLAIN_EDGE_ROW_COUNT:
        return "explain-a"
    if row_number >= EXPLAIN_EDGE_ROW_COUNT + EXPLAIN_TARGET_ROW_COUNT:
        return "explain-z"
    return target
