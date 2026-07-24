"""Behavior-shaped ClickHouse gateways for storage repository scenarios."""

import asyncio
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Final

from taskiq_clickhouse._clickhouse.contracts import ReadWriteGateway
from taskiq_clickhouse._clickhouse.errors import AmbiguousClickHouseError
from taskiq_clickhouse._clickhouse.request import InsertRequest
from tests.integration.storage_repository_contract.response_loss import (
    ResponseLossCase,
)


_OBSERVED_AT_EXPRESSION: Final = "now64(6, 'UTC') AS observed_at"
_EQUAL_OBSERVATION_EXPRESSION: Final = "visible_until AS observed_at"


@dataclass(frozen=True, slots=True)
class QueryCall:
    """One observable production query projection."""

    query: str
    column_formats: Mapping[str, str]


@dataclass(slots=True)
class QueryProjectionGateway:
    """Record query projections while forwarding every real operation."""

    delegate: ReadWriteGateway
    query_calls: list[QueryCall] = field(default_factory=list)

    async def query_rows(
        self,
        query: str,
        *,
        query_parameters: Mapping[str, object] | None = None,
        settings: Mapping[str, object] | None = None,
        column_formats: Mapping[str, str] | None = None,
    ) -> tuple[tuple[object, ...], ...]:
        """Forward a real read while retaining its explicit byte projection."""
        formats = {} if column_formats is None else dict(column_formats)
        self.query_calls.append(QueryCall(query, formats))
        return await self.delegate.query_rows(
            query,
            query_parameters=query_parameters,
            settings=settings,
            column_formats=column_formats,
        )

    async def insert_rows(self, request: InsertRequest) -> None:
        """Forward a native insert unchanged."""
        await self.delegate.insert_rows(request)


@dataclass(slots=True)
class ResponseLossGateway:
    """Lose one selected response after its real storage write commits."""

    delegate: ReadWriteGateway
    case: ResponseLossCase
    insert_count: int = 0
    loss_count: int = 0
    confirmation_count: int = 0

    async def query_rows(
        self,
        query: str,
        *,
        query_parameters: Mapping[str, object] | None = None,
        settings: Mapping[str, object] | None = None,
        column_formats: Mapping[str, str] | None = None,
    ) -> tuple[tuple[object, ...], ...]:
        """Forward reads and count exact acknowledged-write confirmations."""
        if query.startswith("SELECT 1\n"):
            self.confirmation_count += 1
        return await self.delegate.query_rows(
            query,
            query_parameters=query_parameters,
            settings=settings,
            column_formats=column_formats,
        )

    async def insert_rows(self, request: InsertRequest) -> None:
        """Commit a native insert, then lose the selected response once."""
        self.insert_count += 1
        await self.delegate.insert_rows(request)
        if not self.loss_count and self.case.matches(request):
            self.loss_count += 1
            raise AmbiguousClickHouseError


@dataclass(frozen=True, slots=True)
class InsertBarrierGateway:
    """Hold result inserts until every writer has completed allocation."""

    delegate: ReadWriteGateway
    barrier: asyncio.Barrier
    result_table: str

    async def query_rows(
        self,
        query: str,
        *,
        query_parameters: Mapping[str, object] | None = None,
        settings: Mapping[str, object] | None = None,
        column_formats: Mapping[str, str] | None = None,
    ) -> tuple[tuple[object, ...], ...]:
        """Forward allocation, confirmation and latest reads unchanged."""
        return await self.delegate.query_rows(
            query,
            query_parameters=query_parameters,
            settings=settings,
            column_formats=column_formats,
        )

    async def insert_rows(self, request: InsertRequest) -> None:
        """Release same-point result writes as one bounded cohort."""
        if request.table == self.result_table:
            await self.barrier.wait()
        await self.delegate.insert_rows(request)


@dataclass(frozen=True, slots=True)
class EqualObservationGateway:
    """Return a real latest row observed exactly at its visibility boundary."""

    delegate: ReadWriteGateway

    async def query_rows(
        self,
        query: str,
        *,
        query_parameters: Mapping[str, object] | None = None,
        settings: Mapping[str, object] | None = None,
        column_formats: Mapping[str, str] | None = None,
    ) -> tuple[tuple[object, ...], ...]:
        """Substitute only the projected observation clock for equality proof."""
        equal_query = query.replace(
            _OBSERVED_AT_EXPRESSION,
            _EQUAL_OBSERVATION_EXPRESSION,
            1,
        )
        return await self.delegate.query_rows(
            equal_query,
            query_parameters=query_parameters,
            settings=settings,
            column_formats=column_formats,
        )

    async def insert_rows(self, request: InsertRequest) -> None:
        """Forward native inserts unchanged."""
        await self.delegate.insert_rows(request)
