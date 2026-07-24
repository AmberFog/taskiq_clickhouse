"""Administrator probes for permission-denied storage postconditions."""

from typing import Final

from clickhouse_connect.driver.asyncclient import AsyncClient

from tests.integration.backend_failure_matrix.backend_factory import BackendScope


_COUNT_RESULTS: Final = """
SELECT count()
FROM {table}
WHERE namespace = {{namespace:String}}
  AND task_id = {{task_id:String}}
"""


async def count_result_rows(
    client: AsyncClient,
    database: str,
    scope: BackendScope,
    task_id: str,
) -> int:
    """Count every physical row for one isolated namespace/task point."""
    table = scope.storage_layout(database).result_table
    result = await client.query(
        _COUNT_RESULTS.format(table=table.quoted),
        parameters={"namespace": scope.namespace, "task_id": task_id},
    )
    raw_count: object = result.result_rows[0][0]
    if not isinstance(raw_count, int) or isinstance(raw_count, bool):
        message = "ClickHouse count query returned a non-integer"
        raise TypeError(message)
    return raw_count
