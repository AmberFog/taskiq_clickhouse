"""Result and targeted-tombstone storage use cases."""

from __future__ import annotations

from dataclasses import dataclass
from functools import partial
from typing import TYPE_CHECKING, Final

from taskiq_clickhouse._clickhouse import (
    contracts as clickhouse_contracts,
    queries as clickhouse_queries,
)
from taskiq_clickhouse._storage import bindings, projections, queries, result_records
from taskiq_clickhouse.exceptions import ClickHouseDataCorruptionError


if TYPE_CHECKING:
    from taskiq_clickhouse._storage.acknowledged_writer import AcknowledgedWriter
    from taskiq_clickhouse._storage.generation_allocator import GenerationAllocator
    from taskiq_clickhouse._storage.layout import StorageLayout
    from taskiq_clickhouse._storage_policy import StoragePolicy


_TOMBSTONE_INVALID: Final = "tombstone_invalid"
_RESULT_ALLOCATE: Final = "result_allocate"
_RESULT_WRITE: Final = "result_write"
_RESULT_READINESS: Final = "result_readiness"
_RESULT_READ: Final = "result_read"
_RESULT_WITH_LOG_READ: Final = "result_with_log_read"
_TOMBSTONE_WRITE: Final = "tombstone_write"


@dataclass(frozen=True, slots=True, repr=False)
class ResultRepository:
    """Persist and select immutable result/tombstone generations."""

    gateway: clickhouse_contracts.ReadWriteGateway
    layout: StorageLayout
    policy: StoragePolicy
    queries: queries.ResultQueries
    allocator: GenerationAllocator
    writer: AcknowledgedWriter

    async def write_result(
        self,
        task_id: str,
        result_payload: bytes,
        log_payload: bytes,
    ) -> result_records.ResultRecord:
        """Allocate and acknowledge one immutable result generation."""
        namespace = self.policy.namespace.namespace
        allocation = await self.allocator.allocate(
            self.queries.allocator,
            self.queries.bind(bindings.point_parameters(namespace, task_id)),
            operation=_RESULT_ALLOCATE,
            error_type=ClickHouseDataCorruptionError,
        )
        record = result_records.ResultRecord(
            namespace=namespace,
            task_id=task_id,
            generation_at=allocation.generation.generation_at,
            generation_id=allocation.generation.generation_id,
            state=result_records.RESULT_STATE,
            written_at=allocation.written_at,
            visible_until=allocation.deadlines.visible_until,
            purge_at=allocation.deadlines.purge_at,
            result_payload=result_payload,
            log_payload=log_payload,
        )
        await self.writer.write_result(record, operation=_RESULT_WRITE)
        return record

    async def is_result_ready(self, task_id: str) -> bool:
        """Select the latest payload-free state and evaluate visibility."""
        rows = await clickhouse_queries.query_rows(
            self.gateway,
            clickhouse_queries.QueryRequest(
                query=self.queries.readiness,
                operation=_RESULT_READINESS,
                query_parameters=self.queries.bind(
                    bindings.point_parameters(
                        self.policy.namespace.namespace,
                        task_id,
                    ),
                ),
            ),
        )
        selected = projections.decode_projection(
            projections.parse_result_state_rows,
            rows,
            operation=_RESULT_READINESS,
            error_type=ClickHouseDataCorruptionError,
        )
        return selected is not None and selected.is_ready

    async def read_result_no_log(self, task_id: str) -> result_records.ResultRead | None:
        """Select the latest result without reading its log column."""
        return await self._read_result(task_id, with_logs=False, operation=_RESULT_READ)

    async def read_result_with_log(self, task_id: str) -> result_records.ResultRead | None:
        """Select result and log bytes from the same latest physical row."""
        return await self._read_result(task_id, with_logs=True, operation=_RESULT_WITH_LOG_READ)

    async def write_tombstone(self, selected: object) -> result_records.ResultRecord:
        """Acknowledge a tombstone targeted to one already-selected result."""
        validated = _require_selected_scope(selected, self.layout, self.policy)
        if not validated.is_visible_result:
            raise ClickHouseDataCorruptionError(
                _TOMBSTONE_WRITE,
                _TOMBSTONE_INVALID,
            ) from None
        record: result_records.ResultRecord | None = None
        try:
            record = result_records.build_tombstone(validated, self.policy.retention)
        except (TypeError, ValueError):
            record = None
        if record is None:
            raise ClickHouseDataCorruptionError(
                _TOMBSTONE_WRITE,
                _TOMBSTONE_INVALID,
            ) from None
        await self.writer.write_result(record, operation=_TOMBSTONE_WRITE)
        return record

    async def _read_result(
        self,
        task_id: str,
        *,
        with_logs: bool,
        operation: str,
    ) -> result_records.ResultRead | None:
        query = self.queries.with_log if with_logs else self.queries.no_log
        formats = queries.WITH_LOG_COLUMN_FORMATS if with_logs else queries.NO_LOG_COLUMN_FORMATS
        point = result_records.ResultPoint(
            namespace=self.policy.namespace.namespace,
            task_id=task_id,
            result_table=self.layout.result_table,
        )
        rows = await clickhouse_queries.query_rows(
            self.gateway,
            clickhouse_queries.QueryRequest(
                query=query,
                operation=operation,
                query_parameters=self.queries.bind(
                    bindings.point_parameters(point.namespace, point.task_id),
                ),
                column_formats=formats,
            ),
        )
        selected = projections.decode_projection(
            partial(projections.parse_result_rows, point=point, with_logs=with_logs),
            rows,
            operation=operation,
            error_type=ClickHouseDataCorruptionError,
        )
        if selected is None or not selected.is_visible_result:
            return None
        return selected


def _require_selected_scope(
    selected: object,
    layout: StorageLayout,
    policy: StoragePolicy,
) -> result_records.ResultRead:
    if not isinstance(selected, result_records.ResultRead):
        msg = "selected must be a ResultRead"
        raise TypeError(msg)
    point = selected.point
    namespace_mismatch = point.namespace != policy.namespace.namespace
    table_mismatch = point.result_table != layout.result_table
    if namespace_mismatch or table_mismatch:
        msg = "selected result must belong to this repository scope"
        raise ValueError(msg)
    return selected
