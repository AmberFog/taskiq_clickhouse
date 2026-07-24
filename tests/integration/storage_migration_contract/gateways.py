"""Behavior-shaped ClickHouse probe for ambiguous migration responses."""

from collections.abc import Mapping
from dataclasses import dataclass

from taskiq_clickhouse._clickhouse.errors import AmbiguousClickHouseError
from taskiq_clickhouse._clickhouse.request import InsertRequest
from taskiq_clickhouse._schema.layout import METADATA_COLUMN_NAMES, MIGRATION_RECORD_KIND
from taskiq_clickhouse._schema.registry import RegistryGateway
from taskiq_clickhouse._storage.layout import StorageLayout
from tests.integration.storage_migration_contract.cases import (
    LostResponse,
    ResponseLossScenario,
)
from tests.integration.storage_migration_contract.queries import CONFIRMATION_MARKER


@dataclass(slots=True)
class MigrationGatewayProbe:
    """Observe migration operations and optionally lose one committed response."""

    delegate: RegistryGateway
    layout: StorageLayout
    response_loss: ResponseLossScenario | None = None
    loss_count: int = 0
    insert_count: int = 0
    confirmation_count: int = 0

    async def query_rows(
        self,
        query: str,
        *,
        query_parameters: Mapping[str, object] | None = None,
        settings: Mapping[str, object] | None = None,
        column_formats: Mapping[str, str] | None = None,
    ) -> tuple[tuple[object, ...], ...]:
        """Forward reads and count exact metadata confirmations."""
        if CONFIRMATION_MARKER in query:
            self.confirmation_count += 1
        return await self.delegate.query_rows(
            query,
            query_parameters=query_parameters,
            settings=settings,
            column_formats=column_formats,
        )

    async def command(
        self,
        query: str,
        *,
        query_parameters: Mapping[str, object] | None = None,
        settings: Mapping[str, object] | None = None,
    ) -> None:
        """Forward DDL and lose only the selected committed response."""
        await self.delegate.command(
            query,
            query_parameters=query_parameters,
            settings=settings,
        )
        if self._should_lose_ddl(query):
            self.loss_count += 1
            raise AmbiguousClickHouseError

    async def insert_rows(self, request: InsertRequest) -> None:
        """Forward inserts and lose only the selected migration-history response."""
        self.insert_count += 1
        await self.delegate.insert_rows(request)
        if self._should_lose_history(request):
            self.loss_count += 1
            raise AmbiguousClickHouseError

    def _should_lose_ddl(self, query: str) -> bool:
        if self.loss_count or self.response_loss is None:
            return False
        expected_query = {
            LostResponse.RESULT_DDL: self.layout.create_result_query,
            LostResponse.PROGRESS_DDL: self.layout.create_progress_query,
        }.get(self.response_loss.response)
        return query == expected_query

    def _should_lose_history(self, request: InsertRequest) -> bool:
        if self.loss_count or self.response_loss is None:
            return False
        if self.response_loss.response is not LostResponse.HISTORY_INSERT:
            return False
        if tuple(request.column_names) != METADATA_COLUMN_NAMES or len(request.rows) != 1:
            return False
        row = request.rows[0]
        if len(row) != len(METADATA_COLUMN_NAMES):
            return False
        record_kind_index = METADATA_COLUMN_NAMES.index("record_kind")
        return row[record_kind_index] == MIGRATION_RECORD_KIND
