"""Safe ClickHouse row reads shared by storage and schema policies."""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING, Final

from taskiq_clickhouse._clickhouse.errors import (
    AmbiguousClickHouseError,
    DefiniteClickHouseError,
)
from taskiq_clickhouse.exceptions import ClickHouseBackendIOError


if TYPE_CHECKING:
    from collections.abc import Mapping

    from taskiq_clickhouse._clickhouse.contracts import RowsReader


UNCACHED_READ_SETTINGS: Final = MappingProxyType({"use_query_cache": 0})
_Rows = tuple[tuple[object, ...], ...]


@dataclass(frozen=True, slots=True, repr=False)
class QueryRequest:
    """One parameterized row read with an operation-specific error boundary."""

    query: str
    operation: str
    query_parameters: Mapping[str, object] | None = None
    settings: Mapping[str, object] | None = None
    column_formats: Mapping[str, str] | None = None


async def query_rows(reader: RowsReader, request: QueryRequest) -> _Rows:
    """Execute one fresh read and translate classified adapter failures."""
    try:
        return await reader.query_rows(
            request.query,
            query_parameters=request.query_parameters,
            settings=_uncached_read_settings(request.settings),
            column_formats=request.column_formats,
        )
    except AmbiguousClickHouseError:
        reason = "ambiguous_response"
    except DefiniteClickHouseError:
        reason = "database_error"
    raise ClickHouseBackendIOError(request.operation, reason) from None


def _uncached_read_settings(
    settings: Mapping[str, object] | None,
) -> Mapping[str, object]:
    if settings is None:
        return UNCACHED_READ_SETTINGS
    normalized = dict(settings)
    normalized["use_query_cache"] = 0
    return MappingProxyType(normalized)
