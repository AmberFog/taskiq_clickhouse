"""Validate physical-schema inspection against a real ClickHouse catalog."""

from typing import Final

from clickhouse_connect.driver.asyncclient import AsyncClient
import pytest

from taskiq_clickhouse._clickhouse.adapter import ClickHouseGateway
from taskiq_clickhouse._identifiers import Identifier
from taskiq_clickhouse._schema.inspection import SchemaInspector
from taskiq_clickhouse._schema.layout import MetadataLayout


pytestmark = [pytest.mark.clickhouse, pytest.mark.asyncio]

DDL_SETTINGS: Final = {"wait_end_of_query": 1}
DROP_METADATA_TABLE: Final = "DROP TABLE IF EXISTS {table} SYNC"
ADD_UNEXPECTED_COLUMN: Final = """
ALTER TABLE {table}
ADD COLUMN unexpected_metadata UInt8 DEFAULT 0
"""


async def test_metadata_inspection_distinguishes_exact_schema_from_drift(
    clickhouse_client: AsyncClient,
    clickhouse_database: str,
) -> None:
    """Prove exact normalized bootstrap validation and explicit extra-column drift."""
    layout = MetadataLayout(Identifier(clickhouse_database))
    table = layout.table
    contract = layout.contract
    inspector = SchemaInspector(ClickHouseGateway(clickhouse_client))
    await clickhouse_client.command(
        DROP_METADATA_TABLE.format(table=table.quoted),
        settings=DDL_SETTINGS,
    )
    try:
        await clickhouse_client.command(
            layout.create_query,
            parameters=layout.table_parameters,
            settings=DDL_SETTINGS,
        )

        assert await inspector.matches(contract)
        await clickhouse_client.command(
            ADD_UNEXPECTED_COLUMN.format(table=table.quoted),
            settings=DDL_SETTINGS,
        )
        difference = await inspector.diff(contract)

        assert not difference.matches
        assert {mismatch.path for mismatch in difference.mismatches} == {
            "columns.system_count",
            "columns.unexpected",
        }
    finally:
        await clickhouse_client.command(
            DROP_METADATA_TABLE.format(table=table.quoted),
            settings=DDL_SETTINGS,
        )
