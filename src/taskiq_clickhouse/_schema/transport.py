"""Safe schema reads and exact metadata write adaptation."""

from __future__ import annotations

from dataclasses import dataclass
from functools import partial
from typing import TYPE_CHECKING

from taskiq_clickhouse._clickhouse import (
    contracts as clickhouse_contracts,
    errors as clickhouse_errors,
    queries as clickhouse_queries,
    request as clickhouse_request,
)
from taskiq_clickhouse._schema import codec, layout
from taskiq_clickhouse._sql import bind_table
from taskiq_clickhouse._write_acknowledgement import (
    AttemptOutcome,
    acknowledge_bounded_write,
)
from taskiq_clickhouse.exceptions import (
    ClickHouseBackendIOError,
    ClickHouseMigrationError,
)


if TYPE_CHECKING:
    from taskiq_clickhouse._schema.records import MetadataRecord


@dataclass(frozen=True, slots=True)
class ExactMetadataWriter:
    """Write one frozen row with bounded confirmation and one retry."""

    gateway: clickhouse_contracts.ReadWriteGateway
    layout: layout.MetadataLayout

    async def write(self, record: MetadataRecord, *, operation: str) -> None:
        """Confirm ambiguous responses without allocating a new identity."""
        request = self._request(record)
        await acknowledge_bounded_write(
            partial(self._insert_once, request, operation=operation),
            partial(self._confirm_exact, record, operation=operation),
            operation=operation,
        )

    async def _insert_once(
        self,
        request: clickhouse_request.InsertRequest,
        *,
        operation: str,
    ) -> AttemptOutcome:
        error_reason: str | None = None
        try:
            await self.gateway.insert_rows(request)
        except clickhouse_errors.AmbiguousClickHouseError:
            return AttemptOutcome.AMBIGUOUS
        except clickhouse_errors.DefiniteClickHouseError:
            error_reason = "database_error"
        if error_reason is not None:
            raise ClickHouseBackendIOError(operation, error_reason) from None
        return AttemptOutcome.ACKNOWLEDGED

    def _request(self, record: MetadataRecord) -> clickhouse_request.InsertRequest:
        return clickhouse_request.InsertRequest(
            database=self.layout.table.database.value,
            table=self.layout.table.table.value,
            rows=(record.as_row(),),
            column_names=layout.METADATA_COLUMN_NAMES,
            column_type_names=layout.METADATA_COLUMN_TYPES,
            settings=layout.METADATA_WRITE_SETTINGS,
        )

    async def _confirm_exact(self, record: MetadataRecord, *, operation: str) -> bool:
        confirmation_operation = f"{operation}_confirm"
        rows = await clickhouse_queries.query_rows(
            self.gateway,
            clickhouse_queries.QueryRequest(
                query=self.layout.confirmation_query,
                operation=confirmation_operation,
                query_parameters=bind_table(
                    self.layout.table,
                    {
                        "attempt_id": record.attempt_id,
                        "record_key": record.record_key,
                        "record_kind": record.record_kind,
                        "scope": record.scope,
                        "version": record.version,
                    },
                ),
                column_formats=layout.STRING_COLUMN_FORMATS,
            ),
        )
        records = codec.parse_records(rows, operation=confirmation_operation)
        if not records:
            return False
        if any(candidate != record for candidate in records):
            reason = "confirmation_conflict"
            raise ClickHouseMigrationError(operation, reason) from None
        return True
