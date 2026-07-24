"""Validate candidate storage DDL and latest-query behavior on ClickHouse."""

from collections.abc import AsyncIterator
from dataclasses import asdict
from uuid import UUID

from clickhouse_connect.driver.asyncclient import AsyncClient
import pytest
import pytest_asyncio

from tests.integration.evidence import write_evidence
from tests.integration.poc_schema_queries import (
    LOG_PAYLOAD,
    PARTITION_NEWER_GENERATION_ID,
    PROGRESS_COLUMN_TYPES,
    PROGRESS_NEWER_GENERATION_ID,
    RESULT_COLUMN_TYPES,
    RESULT_PAYLOAD,
    STATE_RESULT,
    STATE_TOMBSTONE,
    TARGET_NEWER_GENERATION_ID,
    CandidateTables,
    candidate_tables,
    inspect_candidate_schema,
    observe_acknowledged_result_and_tombstone_visibility,
    observe_explain_pruning,
    observe_latest_across_partitions,
    observe_latest_before_visibility,
    observe_latest_progress_across_partitions,
    observe_no_log_projections,
    observe_part_projection_boundary,
    observe_progress_explain_pruning,
    observe_targeted_state_tie,
)
from tests.integration.settings import ClickHouseTestSettings


pytestmark = [pytest.mark.clickhouse, pytest.mark.asyncio]

RESULT_ENGINE_FULL = (
    "MergeTree PARTITION BY toYYYYMM(purge_at) PRIMARY KEY (namespace, task_id) "
    "ORDER BY (namespace, task_id, generation_at, generation_id, state) "
    "TTL purge_at SETTINGS index_granularity = 8192"
)
PROGRESS_ENGINE_FULL = (
    "MergeTree PARTITION BY toYYYYMM(purge_at) PRIMARY KEY (namespace, task_id) "
    "ORDER BY (namespace, task_id, generation_at, generation_id) "
    "TTL purge_at SETTINGS index_granularity = 8192"
)


@pytest_asyncio.fixture
async def poc_tables(
    clickhouse_client: AsyncClient,
    clickhouse_database: str,
) -> AsyncIterator[CandidateTables]:
    """Yield disposable candidate tables inside the worker database."""
    async with candidate_tables(clickhouse_client, clickhouse_database) as tables:
        yield tables


async def test_candidate_schema_normalizes_every_required_fact(
    clickhouse_client: AsyncClient,
    poc_tables: CandidateTables,
    clickhouse_settings: ClickHouseTestSettings,
) -> None:
    """Pin engines, keys, TTL placement and every candidate column."""
    metadata = await inspect_candidate_schema(clickhouse_client, poc_tables)
    await write_evidence(clickhouse_settings, "poc-schema.json", asdict(metadata))

    assert metadata.result.engine == metadata.progress.engine == "MergeTree"
    assert metadata.result.engine_full == RESULT_ENGINE_FULL
    assert metadata.progress.engine_full == PROGRESS_ENGINE_FULL
    assert metadata.result.partition_key == "toYYYYMM(purge_at)"
    assert metadata.progress.partition_key == "toYYYYMM(purge_at)"
    assert metadata.result.primary_key == "namespace, task_id"
    assert metadata.progress.primary_key == "namespace, task_id"
    assert metadata.result.sorting_key == "namespace, task_id, generation_at, generation_id, state"
    assert metadata.progress.sorting_key == "namespace, task_id, generation_at, generation_id"
    assert metadata.result.sampling_key == ""
    assert metadata.progress.sampling_key == ""
    assert metadata.result.create_table_query.count("TTL purge_at") == 1
    assert metadata.progress.create_table_query.count("TTL purge_at") == 1
    assert tuple((column.name, column.type_name) for column in metadata.result.columns) == RESULT_COLUMN_TYPES
    assert tuple((column.name, column.type_name) for column in metadata.progress.columns) == PROGRESS_COLUMN_TYPES
    assert tuple(column.position for column in metadata.result.columns) == tuple(range(1, len(RESULT_COLUMN_TYPES) + 1))
    assert tuple(column.position for column in metadata.progress.columns) == tuple(
        range(1, len(PROGRESS_COLUMN_TYPES) + 1)
    )
    assert all(column.default_kind == "" for column in metadata.result.columns)
    assert all(column.default_expression == "" for column in metadata.result.columns)
    assert all(column.compression_codec == "" for column in metadata.result.columns)
    assert all(column.default_kind == "" for column in metadata.progress.columns)
    assert all(column.default_expression == "" for column in metadata.progress.columns)
    assert all(column.compression_codec == "" for column in metadata.progress.columns)
    assert tuple(column.name for column in metadata.result.columns if column.in_partition_key) == ("purge_at",)
    assert tuple(column.name for column in metadata.result.columns if column.in_sorting_key) == (
        "namespace",
        "task_id",
        "generation_at",
        "generation_id",
        "state",
    )
    assert tuple(column.name for column in metadata.result.columns if column.in_primary_key) == (
        "namespace",
        "task_id",
    )
    assert all(column.in_sampling_key is False for column in metadata.result.columns)
    assert tuple(column.name for column in metadata.progress.columns if column.in_partition_key) == ("purge_at",)
    assert tuple(column.name for column in metadata.progress.columns if column.in_sorting_key) == (
        "namespace",
        "task_id",
        "generation_at",
        "generation_id",
    )
    assert tuple(column.name for column in metadata.progress.columns if column.in_primary_key) == (
        "namespace",
        "task_id",
    )
    assert all(column.in_sampling_key is False for column in metadata.progress.columns)
    for table, column_types in (
        (metadata.result, RESULT_COLUMN_TYPES),
        (metadata.progress, PROGRESS_COLUMN_TYPES),
    ):
        assert tuple((column.name, column.type_name) for column in table.described_columns) == column_types
        assert all(column.default_type == "" for column in table.described_columns)
        assert all(column.default_expression == "" for column in table.described_columns)
        assert all(column.comment == "" for column in table.described_columns)
        assert all(column.codec_expression == "" for column in table.described_columns)
        assert all(column.ttl_expression == "" for column in table.described_columns)


async def test_latest_order_crosses_reversed_purge_partitions(
    clickhouse_client: AsyncClient,
    poc_tables: CandidateTables,
    clickhouse_settings: ClickHouseTestSettings,
) -> None:
    """Select the newest generation independently of physical partition order."""
    observation = await observe_latest_across_partitions(clickhouse_client, poc_tables)
    await write_evidence(
        clickhouse_settings,
        "poc-result-partitions.json",
        {
            "partitions": observation.partitions,
            "selected_generation_id": str(observation.selected_generation_id),
            "selected_payload_hex": observation.selected_payload.hex(),
        },
    )

    assert observation.selected_payload == b"newer"
    assert observation.selected_generation_id == PARTITION_NEWER_GENERATION_ID
    assert set(observation.partitions) == {"209001", "209002"}


async def test_latest_progress_crosses_reversed_purge_partitions(
    clickhouse_client: AsyncClient,
    poc_tables: CandidateTables,
    clickhouse_settings: ClickHouseTestSettings,
) -> None:
    """Select newest progress independently of physical partition order."""
    observation = await observe_latest_progress_across_partitions(clickhouse_client, poc_tables)
    await write_evidence(
        clickhouse_settings,
        "poc-progress-partitions.json",
        {
            "partitions": observation.partitions,
            "selected_generation_id": str(observation.selected_generation_id),
            "selected_payload_hex": observation.selected_payload.hex(),
        },
    )

    assert observation.selected_payload == b"newer-progress"
    assert observation.selected_generation_id == PROGRESS_NEWER_GENERATION_ID
    assert set(observation.partitions) == {"209007", "209008"}


async def test_acknowledged_result_and_tombstone_are_visible_through_latest_query(
    clickhouse_client: AsyncClient,
    poc_tables: CandidateTables,
    clickhouse_settings: ClickHouseTestSettings,
) -> None:
    """Require immediate latest-path visibility after both accepted writes."""
    del poc_tables
    observation = await observe_acknowledged_result_and_tombstone_visibility(clickhouse_client)
    await write_evidence(
        clickhouse_settings,
        "poc-acknowledged-visibility.json",
        {
            "acknowledged_generation_id": str(observation.acknowledged_generation_id),
            "acknowledged_tombstone_state": observation.acknowledged_tombstone_state,
            "synchronous_payload_hex": observation.synchronous_payload.hex(),
        },
    )

    assert observation.synchronous_payload == b"visible-result"
    assert observation.acknowledged_tombstone_state == STATE_TOMBSTONE
    assert observation.acknowledged_generation_id == UUID("00000000-0000-4000-8000-000000000051")


async def test_tombstone_wins_only_its_target_generation(
    clickhouse_client: AsyncClient,
    poc_tables: CandidateTables,
    clickhouse_settings: ClickHouseTestSettings,
) -> None:
    """Keep newer B visible while state orders tombstone A above result A."""
    observation = await observe_targeted_state_tie(clickhouse_client, poc_tables)
    await write_evidence(
        clickhouse_settings,
        "poc-targeted-tombstone.json",
        {
            "latest_generation_id": str(observation.latest_generation_id),
            "latest_state": observation.latest_state,
            "partitions": observation.partitions,
            "targeted_state": observation.targeted_state,
        },
    )

    assert observation.targeted_state == STATE_TOMBSTONE
    assert observation.latest_generation_id == TARGET_NEWER_GENERATION_ID
    assert observation.latest_state == STATE_RESULT
    assert set(observation.partitions) == {"209004", "209005", "209006"}


async def test_latest_selection_precedes_visibility(
    clickhouse_client: AsyncClient,
    poc_tables: CandidateTables,
    clickhouse_settings: ClickHouseTestSettings,
) -> None:
    """Show that pre-filtering would resurrect an older visible result."""
    del poc_tables
    observation = await observe_latest_before_visibility(clickhouse_client)
    await write_evidence(
        clickhouse_settings,
        "poc-latest-visibility.json",
        {
            "latest_payload_hex": observation.latest_payload.hex(),
            "latest_visible": observation.latest_visible,
            "prefiltered_payload_hex": observation.prefiltered_payload.hex(),
        },
    )

    assert observation.latest_payload == b"newer-expired"
    assert observation.latest_visible is False
    assert observation.prefiltered_payload == b"older-visible"


async def test_no_log_direct_and_alias_projections_are_exact(
    clickhouse_client: AsyncClient,
    poc_tables: CandidateTables,
    clickhouse_settings: ClickHouseTestSettings,
) -> None:
    """Read opaque result bytes without returning the log column."""
    del poc_tables
    observation = await observe_no_log_projections(clickhouse_client)
    await write_evidence(
        clickhouse_settings,
        "poc-no-log-projection.json",
        {
            "aliased_columns": observation.aliased_columns,
            "aliased_payload_hex": observation.aliased_payload.hex(),
            "direct_columns": observation.direct_columns,
            "direct_payload_hex": observation.direct_payload.hex(),
        },
    )

    assert observation.direct_columns == ("result_payload",)
    assert observation.direct_payload == RESULT_PAYLOAD
    assert observation.aliased_columns == ("payload_bytes",)
    assert observation.aliased_payload == RESULT_PAYLOAD
    assert observation.direct_payload != LOG_PAYLOAD


async def test_explain_uses_prefix_pruning_and_reverse_order(
    clickhouse_client: AsyncClient,
    poc_tables: CandidateTables,
    clickhouse_settings: ClickHouseTestSettings,
) -> None:
    """Require the candidate key to support its exact latest query."""
    del poc_tables
    observation = await observe_explain_pruning(clickhouse_client)
    await write_evidence(
        clickhouse_settings,
        "poc-result-explain.json",
        asdict(observation),
    )

    assert observation.uses_primary_key, observation.text
    assert observation.fixes_namespace_and_task, observation.text
    assert observation.reads_in_reverse_order, observation.text


async def test_progress_explain_uses_prefix_pruning_and_reverse_order(
    clickhouse_client: AsyncClient,
    poc_tables: CandidateTables,
    clickhouse_settings: ClickHouseTestSettings,
) -> None:
    """Require the progress key to support its exact latest query."""
    del poc_tables
    observation = await observe_progress_explain_pruning(clickhouse_client)
    await write_evidence(
        clickhouse_settings,
        "poc-progress-explain.json",
        asdict(observation),
    )

    assert observation.uses_primary_key, observation.text
    assert observation.fixes_namespace_and_task, observation.text
    assert observation.reads_in_reverse_order, observation.text


async def test_part_layout_does_not_overclaim_physical_io(
    clickhouse_client: AsyncClient,
    poc_tables: CandidateTables,
    clickhouse_settings: ClickHouseTestSettings,
) -> None:
    """Separate exact network projection from unmeasured physical reads."""
    observation = await observe_part_projection_boundary(clickhouse_client, poc_tables)
    await write_evidence(
        clickhouse_settings,
        "poc-part-projection.json",
        {
            "no_log_columns": observation.no_log_columns,
            "no_log_payload_hex": observation.no_log_payload.hex(),
            "part_types": observation.part_types,
            "partitions": observation.partitions,
            "physical_io_proven_by_projection": observation.physical_io_proven_by_projection,
            "with_log_columns": observation.with_log_columns,
        },
    )

    assert set(observation.part_types) == {"Compact", "Wide"}
    assert set(observation.partitions) == {"209003", "209004"}
    assert observation.no_log_columns == ("result_payload",)
    assert observation.with_log_columns == ("result_payload", "log_payload")
    assert observation.no_log_payload == b"wide-result"
    assert observation.physical_io_proven_by_projection is False
