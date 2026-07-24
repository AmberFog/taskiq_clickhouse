"""Prove rollback-safe result and progress retention against real ClickHouse."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Final
from uuid import uuid4

import pytest

from taskiq_clickhouse._clickhouse.adapter import ClickHouseGateway
from taskiq_clickhouse._clickhouse.request import InsertRequest
from taskiq_clickhouse._schema.codec import parse_server_time
from taskiq_clickhouse._schema.layout import DDL_SETTINGS, SERVER_NOW_QUERY
from taskiq_clickhouse._storage.acknowledged_writer import STORAGE_WRITE_SETTINGS
from taskiq_clickhouse._storage.layout import (
    StorageLayout,
    storage_layout_from_names,
)
from taskiq_clickhouse._storage.queries import (
    PROGRESS_INSERT_COLUMN_NAMES,
    PROGRESS_INSERT_COLUMN_TYPES,
    RESULT_INSERT_COLUMN_NAMES,
    RESULT_INSERT_COLUMN_TYPES,
)
from taskiq_clickhouse._storage.repository import StorageRepository
from taskiq_clickhouse._storage.result_records import RESULT_STATE
from taskiq_clickhouse._storage_policy import NamespaceKey, RetentionPolicy, StoragePolicy
from tests.factories.storage import ProgressRecordFactory, ResultRecordFactory


if TYPE_CHECKING:
    from collections.abc import Mapping
    from datetime import datetime

    from clickhouse_connect.driver.asyncclient import AsyncClient

    from taskiq_clickhouse._clickhouse.contracts import (
        CommandExecutor,
        ReadWriteGateway,
        RowsInserter,
        RowsReader,
    )
    from taskiq_clickhouse._storage.progress_records import ProgressRecord
    from taskiq_clickhouse._storage.result_records import ResultRecord

pytestmark = [pytest.mark.clickhouse, pytest.mark.asyncio]

_RESULT_TTL: Final = timedelta(hours=1)
_PURGE_TTL: Final = timedelta(hours=2)
_FUTURE_GENERATION_OFFSET: Final = timedelta(days=2)
_HISTORICAL_VISIBILITY_OFFSET: Final = timedelta(days=3)
_HISTORICAL_PURGE_OFFSET: Final = timedelta(days=4)
_MICROSECOND: Final = timedelta(microseconds=1)
_SCENARIO_TIMEOUT_SECONDS: Final = 18
_DROP_TABLE: Final = "DROP TABLE IF EXISTS {table} SYNC"
_OBSERVED_AT_EXPRESSION: Final = "now64(6, 'UTC') AS observed_at"
_FIXED_OBSERVED_AT_EXPRESSION: Final = "{fixed_observed_at:DateTime64(6, 'UTC')} AS observed_at"
_HISTORY_QUERY: Final = """
SELECT generation_at, visible_until, purge_at
FROM {table}
PREWHERE namespace = {{namespace:String}} AND task_id = {{task_id:String}}
ORDER BY generation_at
"""


@dataclass(frozen=True, slots=True)
class _FixedObservationGateway:
    """Evaluate latest-row visibility at one explicit server-compatible instant."""

    delegate: ReadWriteGateway
    observed_at: datetime

    async def query_rows(
        self,
        query: str,
        *,
        query_parameters: Mapping[str, object] | None = None,
        settings: Mapping[str, object] | None = None,
        column_formats: Mapping[str, str] | None = None,
    ) -> tuple[tuple[object, ...], ...]:
        """Replace only the latest-read observation expression."""
        fixed_query = query.replace(
            _OBSERVED_AT_EXPRESSION,
            _FIXED_OBSERVED_AT_EXPRESSION,
            1,
        )
        parameters = {} if query_parameters is None else dict(query_parameters)
        parameters["fixed_observed_at"] = self.observed_at
        return await self.delegate.query_rows(
            fixed_query,
            query_parameters=parameters,
            settings=settings,
            column_formats=column_formats,
        )

    async def insert_rows(self, request: InsertRequest) -> None:
        """Forward native inserts unchanged."""
        await self.delegate.insert_rows(request)


async def test_result_and_progress_inherit_purge_floor_after_clock_rollback(
    clickhouse_client: AsyncClient,
    clickhouse_database: str,
) -> None:
    """Keep successor rows until future history cannot physically resurrect."""
    async with asyncio.timeout(_SCENARIO_TIMEOUT_SECONDS):
        layout = _new_layout(clickhouse_database)
        gateway = ClickHouseGateway(clickhouse_client)
        await _create_layout(gateway, layout)
        try:
            now = parse_server_time(await gateway.query_rows(SERVER_NOW_QUERY))
            historical_generation = now + _FUTURE_GENERATION_OFFSET
            historical_visible_until = now + _HISTORICAL_VISIBILITY_OFFSET
            historical_purge_at = now + _HISTORICAL_PURGE_OFFSET
            namespace = f"retention-{uuid4().hex}"
            result_seed = _result_seed(
                namespace,
                now,
                historical_generation,
                historical_visible_until,
                historical_purge_at,
            )
            progress_seed = _progress_seed(
                namespace,
                now,
                historical_generation,
                historical_visible_until,
                historical_purge_at,
            )
            await _insert_seeds(gateway, layout, result_seed, progress_seed)
            repository = _repository(gateway, layout, namespace)

            result = await repository.write_result("result", b"new-result", b"new-log")
            progress = await repository.write_progress("progress", b"new-progress")

            _assert_retention_floor(result, historical_generation, historical_purge_at)
            _assert_retention_floor(progress, historical_generation, historical_purge_at)
            assert result.visible_until == result.written_at + _RESULT_TTL
            assert progress.visible_until == progress.written_at + _RESULT_TTL
            assert historical_visible_until < result.purge_at
            assert historical_visible_until < progress.purge_at
            await _assert_persisted_history(
                gateway,
                layout,
                namespace,
                (result_seed, result),
                (progress_seed, progress),
            )
            await _assert_no_logical_resurrection(
                gateway,
                layout,
                namespace,
                result.visible_until,
                progress.visible_until,
            )
        finally:
            await _drop_layout(gateway, layout)


def _new_layout(database: str) -> StorageLayout:
    suffix = uuid4().hex[:12]
    return storage_layout_from_names(
        database,
        f"retention_result_{suffix}",
        f"retention_progress_{suffix}",
    )


async def _create_layout(gateway: CommandExecutor, layout: StorageLayout) -> None:
    await gateway.command(
        layout.create_result_query,
        query_parameters=layout.create_result_parameters,
        settings=DDL_SETTINGS,
    )
    await gateway.command(
        layout.create_progress_query,
        query_parameters=layout.create_progress_parameters,
        settings=DDL_SETTINGS,
    )


async def _drop_layout(gateway: CommandExecutor, layout: StorageLayout) -> None:
    for table in (layout.progress_table, layout.result_table):
        await gateway.command(
            _DROP_TABLE.format(table=table.quoted),
            settings=DDL_SETTINGS,
        )


def _result_seed(
    namespace: str,
    written_at: datetime,
    generation_at: datetime,
    visible_until: datetime,
    purge_at: datetime,
) -> ResultRecord:
    return ResultRecordFactory.build(
        namespace=namespace,
        task_id="result",
        generation_at=generation_at,
        state=RESULT_STATE,
        written_at=written_at,
        visible_until=visible_until,
        purge_at=purge_at,
        result_payload=b"historical-result",
        log_payload=b"historical-log",
    )


def _progress_seed(
    namespace: str,
    written_at: datetime,
    generation_at: datetime,
    visible_until: datetime,
    purge_at: datetime,
) -> ProgressRecord:
    return ProgressRecordFactory.build(
        namespace=namespace,
        task_id="progress",
        generation_at=generation_at,
        written_at=written_at,
        visible_until=visible_until,
        purge_at=purge_at,
        progress_payload=b"historical-progress",
    )


async def _insert_seeds(
    gateway: RowsInserter,
    layout: StorageLayout,
    result: ResultRecord,
    progress: ProgressRecord,
) -> None:
    await gateway.insert_rows(
        InsertRequest(
            database=layout.database.value,
            table=layout.result_table.table.value,
            rows=(result.as_row(),),
            column_names=RESULT_INSERT_COLUMN_NAMES,
            column_type_names=RESULT_INSERT_COLUMN_TYPES,
            settings=STORAGE_WRITE_SETTINGS,
        ),
    )
    await gateway.insert_rows(
        InsertRequest(
            database=layout.database.value,
            table=layout.progress_table.table.value,
            rows=(progress.as_row(),),
            column_names=PROGRESS_INSERT_COLUMN_NAMES,
            column_type_names=PROGRESS_INSERT_COLUMN_TYPES,
            settings=STORAGE_WRITE_SETTINGS,
        ),
    )


def _repository(
    gateway: ReadWriteGateway,
    layout: StorageLayout,
    namespace: str,
) -> StorageRepository:
    return StorageRepository(
        gateway=gateway,
        layout=layout,
        policy=StoragePolicy(
            namespace=NamespaceKey(namespace),
            retention=RetentionPolicy(
                _RESULT_TTL // _MICROSECOND,
                _PURGE_TTL // _MICROSECOND,
            ),
        ),
    )


def _assert_retention_floor(
    record: ResultRecord | ProgressRecord,
    historical_generation: datetime,
    historical_purge_at: datetime,
) -> None:
    assert record.written_at < historical_generation
    assert record.generation_at == historical_generation + _MICROSECOND
    assert record.purge_at == historical_purge_at
    assert record.purge_at > record.written_at + _PURGE_TTL


async def _assert_persisted_history(
    gateway: RowsReader,
    layout: StorageLayout,
    namespace: str,
    result_history: tuple[ResultRecord, ResultRecord],
    progress_history: tuple[ProgressRecord, ProgressRecord],
) -> None:
    result_rows = await _history_rows(
        gateway,
        layout.result_table.quoted,
        namespace,
        "result",
    )
    progress_rows = await _history_rows(
        gateway,
        layout.progress_table.quoted,
        namespace,
        "progress",
    )
    assert result_rows == (
        tuple((record.generation_at, record.visible_until, record.purge_at) for record in result_history)
    )
    assert progress_rows == (
        tuple((record.generation_at, record.visible_until, record.purge_at) for record in progress_history)
    )


async def _history_rows(
    gateway: RowsReader,
    table: str,
    namespace: str,
    task_id: str,
) -> tuple[tuple[object, ...], ...]:
    return await gateway.query_rows(
        _HISTORY_QUERY.format(table=table),
        query_parameters={"namespace": namespace, "task_id": task_id},
    )


async def _assert_no_logical_resurrection(
    gateway: ReadWriteGateway,
    layout: StorageLayout,
    namespace: str,
    result_expiry: datetime,
    progress_expiry: datetime,
) -> None:
    result_repository = _repository(
        _FixedObservationGateway(gateway, result_expiry),
        layout,
        namespace,
    )
    progress_repository = _repository(
        _FixedObservationGateway(gateway, progress_expiry),
        layout,
        namespace,
    )
    assert await result_repository.read_result_no_log("result") is None
    assert await progress_repository.read_progress("progress") is None
