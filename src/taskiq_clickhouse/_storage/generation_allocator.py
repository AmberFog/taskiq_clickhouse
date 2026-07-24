"""ClickHouse-backed allocation of immutable storage generations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from taskiq_clickhouse._clickhouse import (
    contracts as clickhouse_contracts,
    queries as clickhouse_queries,
)
from taskiq_clickhouse._storage import generation, projections


if TYPE_CHECKING:
    from collections.abc import Mapping

    from taskiq_clickhouse._storage_policy import RetentionPolicy


_ALLOCATION_INVALID = "allocation_invalid"


@dataclass(frozen=True, slots=True, repr=False)
class GenerationAllocator:
    """Allocate generations from server time and retained table history."""

    gateway: clickhouse_contracts.ReadWriteGateway
    retention: RetentionPolicy
    uuid_factory: generation.UUIDFactory

    async def allocate(
        self,
        query: str,
        bindings: Mapping[str, object],
        *,
        operation: str,
        error_type: projections.CorruptionType,
    ) -> generation.WriteAllocation:
        """Read, validate and convert one allocator projection."""
        rows = await clickhouse_queries.query_rows(
            self.gateway,
            clickhouse_queries.QueryRequest(
                query=query,
                operation=operation,
                query_parameters=bindings,
            ),
        )
        observation = projections.decode_projection(
            projections.parse_generation_row,
            rows,
            operation=operation,
            error_type=error_type,
        )
        allocation: generation.WriteAllocation | None = None
        try:
            allocation = generation.allocate_write(
                observation,
                self.retention,
                uuid_factory=self.uuid_factory,
            )
        except (TypeError, ValueError):
            allocation = None
        if allocation is None:
            raise error_type(operation, _ALLOCATION_INVALID) from None
        return allocation
