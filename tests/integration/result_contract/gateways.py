"""Deterministic ClickHouse gateway controls for public result scenarios."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, TypeVar

from taskiq_clickhouse._clickhouse.contracts import ReadWriteGateway
from tests.integration.result_contract.constants import (
    OBSERVED_AT_EXPRESSION,
    VISIBILITY_BOUNDARY_EXPRESSION,
)


if TYPE_CHECKING:
    import asyncio
    from collections.abc import Callable, Mapping

    from taskiq_clickhouse._clickhouse.request import InsertRequest


Rows = tuple[tuple[object, ...], ...]
_GatewayT = TypeVar("_GatewayT", bound=ReadWriteGateway)


@dataclass(slots=True)
class GatewaySwitch:
    """Own one production gateway and an optional test-only decorator."""

    _base: ReadWriteGateway | None = None
    _active: ReadWriteGateway | None = None

    def bind(self, gateway: ReadWriteGateway) -> GatewaySwitch:
        """Bind exactly one production adapter during backend startup."""
        if self._base is not None:
            message = "gateway switch is already bound"
            raise RuntimeError(message)
        self._base = gateway
        self._active = gateway
        return self

    def install(
        self,
        decorator: Callable[[ReadWriteGateway], _GatewayT],
    ) -> _GatewayT:
        """Replace the active decorator while retaining the production base."""
        active = decorator(self._require_base())
        self._active = active
        return active

    def reset(self) -> None:
        """Forward subsequent operations directly to the production adapter."""
        self._active = self._require_base()

    async def query_rows(
        self,
        query: str,
        *,
        query_parameters: Mapping[str, object] | None = None,
        settings: Mapping[str, object] | None = None,
        column_formats: Mapping[str, str] | None = None,
    ) -> Rows:
        """Forward one read through the currently installed gateway."""
        return await self._require_active().query_rows(
            query,
            query_parameters=query_parameters,
            settings=settings,
            column_formats=column_formats,
        )

    async def insert_rows(self, request: InsertRequest) -> None:
        """Forward one insert through the currently installed gateway."""
        await self._require_active().insert_rows(request)

    def _require_base(self) -> ReadWriteGateway:
        if self._base is None:
            message = "gateway switch is not bound"
            raise RuntimeError(message)
        return self._base

    def _require_active(self) -> ReadWriteGateway:
        if self._active is None:
            message = "gateway switch is not bound"
            raise RuntimeError(message)
        return self._active


@dataclass(slots=True)
class ForwardingGateway:
    """Forward every schema operation to one real ClickHouse gateway."""

    delegate: ReadWriteGateway

    async def query_rows(
        self,
        query: str,
        *,
        query_parameters: Mapping[str, object] | None = None,
        settings: Mapping[str, object] | None = None,
        column_formats: Mapping[str, str] | None = None,
    ) -> Rows:
        """Forward one materialized query unchanged."""
        return await self.delegate.query_rows(
            query,
            query_parameters=query_parameters,
            settings=settings,
            column_formats=column_formats,
        )

    async def insert_rows(self, request: InsertRequest) -> None:
        """Forward one native insert unchanged."""
        await self.delegate.insert_rows(request)


@dataclass(slots=True)
class CorruptLogInsertGateway(ForwardingGateway):
    """Replace only the serialized log cell of a public result write."""

    corrupt_payload: bytes
    corrupted: bool = False

    async def insert_rows(self, request: InsertRequest) -> None:
        """Forward a copy containing one intentionally malformed log blob."""
        column_names = tuple(request.column_names)
        if "log_payload" not in column_names:
            await ForwardingGateway.insert_rows(self, request)
            return
        log_index = column_names.index("log_payload")
        corrupted_rows = tuple((*row[:log_index], self.corrupt_payload, *row[log_index + 1 :]) for row in request.rows)
        self.corrupted = True
        await ForwardingGateway.insert_rows(
            self,
            replace(request, rows=corrupted_rows),
        )


@dataclass(slots=True)
class VisibilityBoundaryGateway(ForwardingGateway):
    """Observe a real selected row exactly at its exclusive visibility tick."""

    async def query_rows(
        self,
        query: str,
        *,
        query_parameters: Mapping[str, object] | None = None,
        settings: Mapping[str, object] | None = None,
        column_formats: Mapping[str, str] | None = None,
    ) -> Rows:
        """Substitute only the projected server observation instant."""
        boundary_query = query.replace(
            OBSERVED_AT_EXPRESSION,
            VISIBILITY_BOUNDARY_EXPRESSION,
            1,
        )
        return await ForwardingGateway.query_rows(
            self,
            boundary_query,
            query_parameters=query_parameters,
            settings=settings,
            column_formats=column_formats,
        )


@dataclass(slots=True)
class ReadBarrierGateway(ForwardingGateway):
    """Hold one completed real selection until every consumer has selected."""

    target_query: str
    barrier: asyncio.Barrier
    crossed: bool = False

    async def query_rows(
        self,
        query: str,
        *,
        query_parameters: Mapping[str, object] | None = None,
        settings: Mapping[str, object] | None = None,
        column_formats: Mapping[str, str] | None = None,
    ) -> Rows:
        """Gate only the first matching read after its rows are materialized."""
        rows = await ForwardingGateway.query_rows(
            self,
            query,
            query_parameters=query_parameters,
            settings=settings,
            column_formats=column_formats,
        )
        if query == self.target_query and not self.crossed:
            self.crossed = True
            await self.barrier.wait()
        return rows


@dataclass(slots=True)
class CapturedReadGateway(ForwardingGateway):
    """Expose one captured real selection before returning it to the repository."""

    target_query: str
    captured: asyncio.Event
    release: asyncio.Event
    crossed: bool = False

    async def query_rows(
        self,
        query: str,
        *,
        query_parameters: Mapping[str, object] | None = None,
        settings: Mapping[str, object] | None = None,
        column_formats: Mapping[str, str] | None = None,
    ) -> Rows:
        """Pause the first matching materialized read at a deterministic gate."""
        rows = await ForwardingGateway.query_rows(
            self,
            query,
            query_parameters=query_parameters,
            settings=settings,
            column_formats=column_formats,
        )
        if query == self.target_query and not self.crossed:
            self.crossed = True
            self.captured.set()
            await self.release.wait()
        return rows
