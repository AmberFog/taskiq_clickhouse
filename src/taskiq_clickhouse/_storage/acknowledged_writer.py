"""Bounded, exact-identity acknowledgement for immutable storage writes."""

from __future__ import annotations

from dataclasses import dataclass
from functools import partial
from types import MappingProxyType
from typing import TYPE_CHECKING, Final

from taskiq_clickhouse._clickhouse import (
    contracts as clickhouse_contracts,
    errors as clickhouse_errors,
    queries as clickhouse_queries,
    request as clickhouse_request,
)
from taskiq_clickhouse._storage import bindings, progress_records, projections, queries, result_records
from taskiq_clickhouse._write_acknowledgement import (
    AttemptOutcome,
    acknowledge_bounded_write,
)
from taskiq_clickhouse.exceptions import (
    ClickHouseBackendIOError,
    ClickHouseDataCorruptionError,
    ClickHouseProgressError,
)


if TYPE_CHECKING:
    from taskiq_clickhouse._storage.layout import StorageLayout


STORAGE_WRITE_SETTINGS: Final = MappingProxyType(
    {
        "async_insert": 0,
        "wait_for_async_insert": 1,
        "wait_end_of_query": 1,
    },
)
_DATABASE_ERROR: Final = "database_error"
_PROGRESS_WRITE: Final = "progress_write"


@dataclass(frozen=True, slots=True, repr=False)
class AcknowledgedWriter:
    """Insert one frozen row with bounded exact-identity confirmation."""

    gateway: clickhouse_contracts.ReadWriteGateway
    layout: StorageLayout
    result_queries: queries.ResultQueries
    progress_queries: queries.ProgressQueries

    async def write_result(self, record: result_records.ResultRecord, *, operation: str) -> None:
        """Acknowledge one result or targeted tombstone row."""
        request = clickhouse_request.InsertRequest(
            database=self.layout.database.value,
            table=self.layout.result_table.table.value,
            rows=(record.as_row(),),
            column_names=queries.RESULT_INSERT_COLUMN_NAMES,
            column_type_names=queries.RESULT_INSERT_COLUMN_TYPES,
            settings=STORAGE_WRITE_SETTINGS,
        )
        await acknowledge_bounded_write(
            partial(self._insert_once, request, operation=operation),
            partial(self._confirm_result, record, operation=operation),
            operation=operation,
        )

    async def write_progress(self, record: progress_records.ProgressRecord) -> None:
        """Acknowledge one progress row."""
        request = clickhouse_request.InsertRequest(
            database=self.layout.database.value,
            table=self.layout.progress_table.table.value,
            rows=(record.as_row(),),
            column_names=queries.PROGRESS_INSERT_COLUMN_NAMES,
            column_type_names=queries.PROGRESS_INSERT_COLUMN_TYPES,
            settings=STORAGE_WRITE_SETTINGS,
        )
        await acknowledge_bounded_write(
            partial(self._insert_once, request, operation=_PROGRESS_WRITE),
            partial(self._confirm_progress, record),
            operation=_PROGRESS_WRITE,
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
            error_reason = _DATABASE_ERROR
        if error_reason is not None:
            raise ClickHouseBackendIOError(operation, error_reason) from None
        return AttemptOutcome.ACKNOWLEDGED

    async def _confirm_result(
        self,
        record: result_records.ResultRecord,
        *,
        operation: str,
    ) -> bool:
        confirmation_operation = f"{operation}_confirm"
        rows = await clickhouse_queries.query_rows(
            self.gateway,
            clickhouse_queries.QueryRequest(
                query=self.result_queries.confirmation,
                operation=confirmation_operation,
                query_parameters=self.result_queries.bind(
                    bindings.result_confirmation_parameters(record),
                ),
            ),
        )
        return projections.decode_projection(
            projections.parse_confirmation_rows,
            rows,
            operation=confirmation_operation,
            error_type=ClickHouseDataCorruptionError,
        )

    async def _confirm_progress(self, record: progress_records.ProgressRecord) -> bool:
        confirmation_operation = f"{_PROGRESS_WRITE}_confirm"
        rows = await clickhouse_queries.query_rows(
            self.gateway,
            clickhouse_queries.QueryRequest(
                query=self.progress_queries.confirmation,
                operation=confirmation_operation,
                query_parameters=self.progress_queries.bind(
                    bindings.progress_confirmation_parameters(record),
                ),
            ),
        )
        return projections.decode_projection(
            projections.parse_confirmation_rows,
            rows,
            operation=confirmation_operation,
            error_type=ClickHouseProgressError,
        )
