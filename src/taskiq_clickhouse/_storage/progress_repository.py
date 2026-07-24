"""Progress storage use cases."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

from taskiq_clickhouse._clickhouse import (
    contracts as clickhouse_contracts,
    queries as clickhouse_queries,
)
from taskiq_clickhouse._storage import bindings, progress_records, projections, queries
from taskiq_clickhouse.exceptions import ClickHouseProgressError


if TYPE_CHECKING:
    from taskiq_clickhouse._storage.acknowledged_writer import AcknowledgedWriter
    from taskiq_clickhouse._storage.generation_allocator import GenerationAllocator
    from taskiq_clickhouse._storage_policy import StoragePolicy


_PROGRESS_ALLOCATE: Final = "progress_allocate"
_PROGRESS_READ: Final = "progress_read"


@dataclass(frozen=True, slots=True, repr=False)
class ProgressRepository:
    """Persist and select immutable progress generations."""

    gateway: clickhouse_contracts.ReadWriteGateway
    policy: StoragePolicy
    queries: queries.ProgressQueries
    allocator: GenerationAllocator
    writer: AcknowledgedWriter

    async def write_progress(
        self,
        task_id: str,
        progress_payload: bytes,
    ) -> progress_records.ProgressRecord:
        """Allocate and acknowledge one immutable progress generation."""
        namespace = self.policy.namespace.namespace
        allocation = await self.allocator.allocate(
            self.queries.allocator,
            self.queries.bind(bindings.point_parameters(namespace, task_id)),
            operation=_PROGRESS_ALLOCATE,
            error_type=ClickHouseProgressError,
        )
        record = progress_records.ProgressRecord(
            namespace=namespace,
            task_id=task_id,
            generation_at=allocation.generation.generation_at,
            generation_id=allocation.generation.generation_id,
            written_at=allocation.written_at,
            visible_until=allocation.deadlines.visible_until,
            purge_at=allocation.deadlines.purge_at,
            progress_payload=progress_payload,
        )
        await self.writer.write_progress(record)
        return record

    async def read_progress(self, task_id: str) -> progress_records.ProgressRead | None:
        """Select the latest progress and evaluate its logical visibility."""
        rows = await clickhouse_queries.query_rows(
            self.gateway,
            clickhouse_queries.QueryRequest(
                query=self.queries.latest,
                operation=_PROGRESS_READ,
                query_parameters=self.queries.bind(
                    bindings.point_parameters(
                        self.policy.namespace.namespace,
                        task_id,
                    ),
                ),
                column_formats=queries.PROGRESS_COLUMN_FORMATS,
            ),
        )
        selected = projections.decode_projection(
            projections.parse_progress_rows,
            rows,
            operation=_PROGRESS_READ,
            error_type=ClickHouseProgressError,
        )
        if selected is None or not selected.is_visible:
            return None
        return selected
