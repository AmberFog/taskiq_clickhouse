"""Consumer-facing ClickHouse capability contracts."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol


if TYPE_CHECKING:
    from collections.abc import Mapping

    from taskiq_clickhouse._clickhouse.request import InsertRequest


class RowsReader(Protocol):
    """Materialize ClickHouse rows without exposing driver result objects."""

    async def query_rows(
        self,
        query: str,
        *,
        query_parameters: Mapping[str, object] | None = None,
        settings: Mapping[str, object] | None = None,
        column_formats: Mapping[str, str] | None = None,
    ) -> tuple[tuple[object, ...], ...]:
        """Execute one read and return immutable row containers."""
        ...


class CommandExecutor(Protocol):
    """Execute one acknowledged ClickHouse command."""

    async def command(
        self,
        query: str,
        *,
        query_parameters: Mapping[str, object] | None = None,
        settings: Mapping[str, object] | None = None,
    ) -> None:
        """Wait for the complete command response."""
        ...


class RowsInserter(Protocol):
    """Insert native rows with an explicit physical column contract."""

    async def insert_rows(self, request: InsertRequest) -> None:
        """Execute one native insert request."""
        ...


class ReadWriteGateway(RowsReader, RowsInserter, Protocol):
    """Compose the two capabilities required by append-only stores."""
