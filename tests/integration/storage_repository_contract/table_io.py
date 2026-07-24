"""Native ClickHouse writes and independent physical storage probes."""

from datetime import datetime
from typing import Final
from uuid import UUID

from taskiq_clickhouse._clickhouse.contracts import RowsInserter, RowsReader
from taskiq_clickhouse._clickhouse.request import InsertRequest
from taskiq_clickhouse._schema.codec import parse_server_time
from taskiq_clickhouse._schema.layout import SERVER_NOW_QUERY
from taskiq_clickhouse._storage.acknowledged_writer import STORAGE_WRITE_SETTINGS
from taskiq_clickhouse._storage.layout import StorageLayout
from taskiq_clickhouse._storage.progress_records import ProgressRecord
from taskiq_clickhouse._storage.queries import (
    PROGRESS_INSERT_COLUMN_NAMES,
    PROGRESS_INSERT_COLUMN_TYPES,
    RESULT_INSERT_COLUMN_NAMES,
    RESULT_INSERT_COLUMN_TYPES,
)
from taskiq_clickhouse._storage.result_records import ResultRecord
from tests.integration.storage_repository_contract.repository_actions import (
    RepositoryHarness,
)


_LATEST_IDENTITY_WIDTH: Final = 2
_ACTIVE_PARTITIONS_QUERY: Final = """
SELECT partition
FROM system.parts
WHERE active
  AND database = {database:String}
  AND table = {table:String}
ORDER BY partition
"""
_EXPECTED_LATEST_QUERY: Final = """
SELECT generation_id, result_payload
FROM {table}
PREWHERE namespace = {{namespace:String}} AND task_id = {{task_id:String}}
ORDER BY namespace DESC, task_id DESC,
         generation_at DESC, generation_id DESC, state DESC
LIMIT 1
"""


async def server_now(gateway: RowsReader) -> datetime:
    """Read and parse one authoritative server timestamp."""
    return parse_server_time(await gateway.query_rows(SERVER_NOW_QUERY))


async def insert_result_records(
    gateway: RowsInserter,
    layout: StorageLayout,
    records: tuple[ResultRecord, ...],
) -> None:
    """Insert exact result records through the production native contract."""
    await gateway.insert_rows(
        InsertRequest(
            database=layout.database.value,
            table=layout.result_table.table.value,
            rows=tuple(record.as_row() for record in records),
            column_names=RESULT_INSERT_COLUMN_NAMES,
            column_type_names=RESULT_INSERT_COLUMN_TYPES,
            settings=STORAGE_WRITE_SETTINGS,
        ),
    )


async def insert_progress_records(
    gateway: RowsInserter,
    layout: StorageLayout,
    records: tuple[ProgressRecord, ...],
) -> None:
    """Insert exact progress records through the production native contract."""
    await gateway.insert_rows(
        InsertRequest(
            database=layout.database.value,
            table=layout.progress_table.table.value,
            rows=tuple(record.as_row() for record in records),
            column_names=PROGRESS_INSERT_COLUMN_NAMES,
            column_type_names=PROGRESS_INSERT_COLUMN_TYPES,
            settings=STORAGE_WRITE_SETTINGS,
        ),
    )


async def active_partitions(
    gateway: RowsReader,
    table: str,
    database: str,
) -> tuple[str, ...]:
    """Return active partitions for one exact physical table."""
    rows = await gateway.query_rows(
        _ACTIVE_PARTITIONS_QUERY,
        query_parameters={"database": database, "table": table},
    )
    return tuple(str(row[0]) for row in rows)


async def expected_latest(
    gateway: RowsReader,
    harness: RepositoryHarness,
    task_id: str,
) -> tuple[UUID, bytes]:
    """Read the physical winner independently of repository decoding."""
    rows = await gateway.query_rows(
        _EXPECTED_LATEST_QUERY.format(table=harness.layout.result_table.quoted),
        query_parameters={"namespace": harness.namespace, "task_id": task_id},
        column_formats={"result_payload": "bytes"},
    )
    if len(rows) != 1 or len(rows[0]) != _LATEST_IDENTITY_WIDTH:
        message = "latest identity query returned an invalid shape"
        raise TypeError(message)
    generation_id, payload = rows[0]
    if not isinstance(generation_id, UUID) or not isinstance(payload, bytes):
        message = "latest identity query returned invalid native values"
        raise TypeError(message)
    return generation_id, payload
