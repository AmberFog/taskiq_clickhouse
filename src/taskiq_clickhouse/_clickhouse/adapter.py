"""Concrete clickhouse-connect adapter for package-owned capabilities."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, TypeAlias, cast

from clickhouse_connect.driver.exceptions import (
    ClickHouseError,
    OperationalError,
    StreamFailureError,
)

from taskiq_clickhouse._clickhouse.errors import (
    AmbiguousClickHouseError,
    DefiniteClickHouseError,
)


if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from typing import Any

    from clickhouse_connect.driver.asyncclient import AsyncClient

    from taskiq_clickhouse._clickhouse.request import InsertRequest


_Rows: TypeAlias = tuple[tuple[object, ...], ...]


@dataclass(frozen=True, slots=True, repr=False)
class ClickHouseGateway:
    """Adapt one owned async client to package-owned capabilities."""

    client: AsyncClient

    async def query_rows(
        self,
        query: str,
        *,
        query_parameters: Mapping[str, object] | None = None,
        settings: Mapping[str, object] | None = None,
        column_formats: Mapping[str, str] | None = None,
    ) -> _Rows:
        """Execute a query and detach driver failures from policy code."""
        failure: AmbiguousClickHouseError | DefiniteClickHouseError
        try:
            query_result = await self.client.query(
                query,
                parameters=_copy_mapping(query_parameters),
                settings=_copy_mapping(settings),
                column_formats=cast(
                    "dict[str, str | dict[str, str]] | None",
                    _copy_mapping(column_formats),
                ),
            )
        except Exception as error:  # noqa: BLE001 - the adapter owns ordinary driver failures.
            failure = _classify_driver_error(error)
        else:
            rows = _materialize_rows(query_result)
            if rows is not None:
                return rows
            failure = DefiniteClickHouseError()
        raise failure

    async def command(
        self,
        query: str,
        *,
        query_parameters: Mapping[str, object] | None = None,
        settings: Mapping[str, object] | None = None,
    ) -> None:
        """Execute a command and detach driver failures from policy code."""
        failure: AmbiguousClickHouseError | DefiniteClickHouseError
        try:
            await self.client.command(
                query,
                parameters=_copy_mapping(query_parameters),
                settings=_copy_mapping(settings),
            )
        except Exception as error:  # noqa: BLE001 - the adapter owns ordinary driver failures.
            failure = _classify_driver_error(error)
        else:
            return
        raise failure

    async def insert_rows(self, request: InsertRequest) -> None:
        """Execute a validated native insert and classify its outcome."""
        failure: AmbiguousClickHouseError | DefiniteClickHouseError
        try:
            await self.client.insert(
                table=request.table,
                database=request.database,
                data=request.rows,
                column_names=request.column_names,
                column_type_names=request.column_type_names,
                settings=dict(request.settings),
            )
        except Exception as error:  # noqa: BLE001 - the adapter owns ordinary driver failures.
            failure = _classify_driver_error(error)
        else:
            return
        raise failure


def _copy_mapping(
    candidate: Mapping[str, object] | None,
) -> dict[str, object] | None:
    if candidate is None:
        return None
    return dict(candidate)


def _classify_driver_error(
    error: Exception,
) -> AmbiguousClickHouseError | DefiniteClickHouseError:
    """Map every ordinary driver failure to the package transport taxonomy."""
    if isinstance(error, (OperationalError, StreamFailureError)):
        return AmbiguousClickHouseError()
    if isinstance(error, ClickHouseError):
        return DefiniteClickHouseError()
    return AmbiguousClickHouseError()


def _materialize_rows(candidate: object) -> _Rows | None:
    """Detach malformed driver response details at the adapter boundary."""
    try:
        driver_result = cast("Any", candidate)
        result_rows = cast("Sequence[Sequence[object]]", driver_result.result_rows)
        return tuple(tuple(row) for row in result_rows)
    except Exception:  # noqa: BLE001 - the external response object is untrusted here.
        return None
