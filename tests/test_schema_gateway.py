"""Test the package-owned ClickHouse adapter and native insert request."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, cast

from clickhouse_connect.driver.exceptions import (
    DatabaseError,
    OperationalError,
    StreamFailureError,
)
import pytest

from taskiq_clickhouse._clickhouse.adapter import ClickHouseGateway
from taskiq_clickhouse._clickhouse.errors import (
    AmbiguousClickHouseError,
    DefiniteClickHouseError,
)
from taskiq_clickhouse._clickhouse.queries import QueryRequest
from taskiq_clickhouse._clickhouse.request import InsertRequest


if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

    from clickhouse_connect.driver.asyncclient import AsyncClient


@dataclass(slots=True)
class _QueryResult:
    result_rows: list[list[object]]


class _MalformedQueryResult:
    @property
    def result_rows(self) -> object:
        """Raise driver-shaped unsafe text while materializing rows."""
        message = "unsafe response details"
        raise RuntimeError(message)


class _FatalDriverSignal(BaseException):
    """Represent a process-level signal that an adapter must not classify."""


@dataclass(slots=True)
class _DriverClient:
    query_calls: list[dict[str, object]] = field(default_factory=list)
    command_calls: list[dict[str, object]] = field(default_factory=list)
    insert_calls: list[dict[str, object]] = field(default_factory=list)
    query_error: BaseException | None = None
    command_error: BaseException | None = None
    insert_error: BaseException | None = None
    query_result: object = field(default_factory=lambda: _QueryResult([[1, "row"]]))

    async def query(self, query: str, **options: object) -> object:
        self.query_calls.append({"query": query, **options})
        if self.query_error is not None:
            raise self.query_error
        return self.query_result

    async def command(self, query: str, **options: object) -> str:
        self.command_calls.append({"query": query, **options})
        if self.command_error is not None:
            raise self.command_error
        return "ok"

    async def insert(self, **options: object) -> object:
        self.insert_calls.append(options)
        if self.insert_error is not None:
            raise self.insert_error
        return object()


@pytest.mark.asyncio
async def test_gateway_materializes_rows_and_copies_query_options() -> None:
    """Hide mutable driver results and never retain caller mappings."""
    driver = _DriverClient()
    gateway = ClickHouseGateway(cast("AsyncClient", driver))
    query_parameters = {"value": 1}
    settings = {"wait_end_of_query": 1}
    formats = {"payload": "bytes"}

    rows = await gateway.query_rows(
        "SELECT 1",
        query_parameters=query_parameters,
        settings=settings,
        column_formats=formats,
    )
    query_parameters["value"] = 2

    assert rows == ((1, "row"),)
    assert driver.query_calls == [
        {
            "query": "SELECT 1",
            "parameters": {"value": 1},
            "settings": {"wait_end_of_query": 1},
            "column_formats": {"payload": "bytes"},
        },
    ]


@pytest.mark.asyncio
async def test_gateway_preserves_none_query_and_command_options() -> None:
    """Pass explicit None when no query or command options are supplied."""
    driver = _DriverClient()
    gateway = ClickHouseGateway(cast("AsyncClient", driver))

    assert await gateway.query_rows("SELECT 1") == ((1, "row"),)
    await gateway.command("CREATE TABLE example (value UInt8)")

    assert driver.query_calls[0]["parameters"] is None
    assert driver.query_calls[0]["settings"] is None
    assert driver.query_calls[0]["column_formats"] is None
    assert driver.command_calls == [
        {
            "query": "CREATE TABLE example (value UInt8)",
            "parameters": None,
            "settings": None,
        },
    ]


@pytest.mark.asyncio
async def test_gateway_issues_explicit_native_insert_request() -> None:
    """Forward every stable insert field without a DESCRIBE lookup."""
    driver = _DriverClient()
    gateway = ClickHouseGateway(cast("AsyncClient", driver))
    request = InsertRequest(
        database="test_db",
        table="metadata",
        rows=(("kind", 1),),
        column_names=("kind", "version"),
        column_type_names=("String", "UInt32"),
        settings={"async_insert": 0},
    )

    await gateway.insert_rows(request)

    assert driver.insert_calls == [
        {
            "table": "metadata",
            "database": "test_db",
            "data": (("kind", 1),),
            "column_names": ("kind", "version"),
            "column_type_names": ("String", "UInt32"),
            "settings": {"async_insert": 0},
        },
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["query", "command", "insert"])
@pytest.mark.parametrize(
    ("raw_error_factory", "expected_error"),
    [
        (lambda: OperationalError("transport details"), AmbiguousClickHouseError),
        (lambda: StreamFailureError("stream details"), AmbiguousClickHouseError),
        (lambda: DatabaseError("database details"), DefiniteClickHouseError),
        (lambda: RuntimeError("unexpected details"), AmbiguousClickHouseError),
    ],
)
async def test_gateway_classifies_driver_failures_at_the_adapter_boundary(
    operation: str,
    raw_error_factory: Callable[[], BaseException],
    expected_error: type[Exception],
) -> None:
    """Expose stable package errors without retaining unsafe driver context."""
    driver = _DriverClient()
    setattr(driver, f"{operation}_error", raw_error_factory())
    gateway = ClickHouseGateway(cast("AsyncClient", driver))

    with pytest.raises(expected_error) as raised:
        await _invoke(gateway, operation)

    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None
    assert "details" not in str(raised.value)


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["query", "command", "insert"])
async def test_gateway_preserves_fatal_driver_signals(
    operation: str,
) -> None:
    """Keep non-ordinary process signals outside the stable I/O taxonomy."""
    fatal = _FatalDriverSignal()
    driver = _DriverClient()
    setattr(driver, f"{operation}_error", fatal)

    with pytest.raises(_FatalDriverSignal) as raised:
        await _invoke(ClickHouseGateway(cast("AsyncClient", driver)), operation)

    assert raised.value is fatal


@pytest.mark.asyncio
async def test_gateway_classifies_malformed_driver_result_without_context() -> None:
    """Keep driver response-object failures behind the adapter taxonomy."""
    driver = _DriverClient(query_result=_MalformedQueryResult())

    with pytest.raises(DefiniteClickHouseError) as raised:
        await ClickHouseGateway(cast("AsyncClient", driver)).query_rows("SELECT 1")

    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None


def test_insert_request_defensively_copies_every_mutable_input() -> None:
    """Freeze rows, physical columns and settings at construction time."""
    rows: list[list[object]] = [["kind", 1]]
    column_names = ["kind", "version"]
    column_types = ["String", "UInt32"]
    settings: dict[str, object] = {"async_insert": 0}

    request = InsertRequest(
        database="test_db",
        table="metadata",
        rows=rows,
        column_names=column_names,
        column_type_names=column_types,
        settings=settings,
    )
    rows[0][0] = "changed"
    rows.append(["extra", 2])
    column_names[0] = "changed"
    column_types[0] = "UInt8"
    settings["async_insert"] = 1

    assert request.rows == (("kind", 1),)
    assert request.column_names == ("kind", "version")
    assert request.column_type_names == ("String", "UInt32")
    assert dict(request.settings) == {"async_insert": 0}
    with pytest.raises(TypeError, match="does not support item assignment"):
        cast("dict[str, object]", request.settings)["async_insert"] = 1


def test_clickhouse_boundary_values_have_secret_safe_representations() -> None:
    """Keep clients, parameter values and inserted payloads out of diagnostics."""
    secret = "password=boundary-secret"  # noqa: S105  # pragma: allowlist secret
    driver = _DriverClient()
    request = InsertRequest(
        database="test_db",
        table="metadata",
        rows=((secret,),),
        column_names=("payload",),
        column_type_names=("String",),
        settings={},
    )
    query = QueryRequest(
        "SELECT {value:String}",
        operation="metadata_read",
        query_parameters={"value": secret},
    )

    assert secret not in repr(request)
    assert secret not in repr(query)
    assert secret not in repr(ClickHouseGateway(cast("AsyncClient", driver)))


@pytest.mark.parametrize(
    ("request_factory", "error_type", "message"),
    [
        (
            lambda: InsertRequest("db", "table", ((1,),), ("a", "b"), ("UInt8",), {}),
            ValueError,
            "equal length",
        ),
        (
            lambda: InsertRequest("db", "table", ((1,),), ("a", "b"), ("UInt8", "UInt8"), {}),
            ValueError,
            "column count",
        ),
        (
            lambda: InsertRequest("db", "table", (), ("value",), ("UInt8",), {}),
            ValueError,
            "rows must not be empty",
        ),
        (
            lambda: InsertRequest("db", "table", ((1,),), (), (), {}),
            ValueError,
            "must not be empty",
        ),
        (
            lambda: InsertRequest(
                "db",
                "table",
                cast("Sequence[Sequence[object]]", object()),
                ("value",),
                ("UInt8",),
                {},
            ),
            TypeError,
            "rows must be a sequence",
        ),
        (
            lambda: InsertRequest(
                "db",
                "table",
                (cast("Sequence[object]", "not-a-row"),),
                ("value",),
                ("UInt8",),
                {},
            ),
            TypeError,
            "each row must be a sequence",
        ),
        (
            lambda: InsertRequest(
                "db",
                "table",
                ((1,),),
                cast("Sequence[str]", "not-columns"),
                ("UInt8",),
                {},
            ),
            TypeError,
            "column_names must be a sequence",
        ),
        (
            lambda: InsertRequest(
                "db",
                "table",
                ((1,),),
                (cast("str", object()),),
                ("UInt8",),
                {},
            ),
            TypeError,
            "non-empty strings",
        ),
        (
            lambda: InsertRequest(
                "db",
                "table",
                ((1,),),
                ("value",),
                ("UInt8",),
                cast("Mapping[str, object]", ()),
            ),
            TypeError,
            "settings must be a mapping",
        ),
        (
            lambda: InsertRequest(
                "db",
                "table",
                ((1,),),
                ("value",),
                ("UInt8",),
                cast("Mapping[str, object]", {1: "invalid"}),
            ),
            TypeError,
            "settings keys must be strings",
        ),
        (
            lambda: InsertRequest(
                cast("str", 1),
                "table",
                ((1,),),
                ("value",),
                ("UInt8",),
                {},
            ),
            TypeError,
            "database must be a string",
        ),
        (
            lambda: InsertRequest("", "table", ((1,),), ("value",), ("UInt8",), {}),
            ValueError,
            "database must not be empty",
        ),
        (
            lambda: InsertRequest(
                "db",
                cast("str", 1),
                ((1,),),
                ("value",),
                ("UInt8",),
                {},
            ),
            TypeError,
            "table must be a string",
        ),
        (
            lambda: InsertRequest("db", "", ((1,),), ("value",), ("UInt8",), {}),
            ValueError,
            "table must not be empty",
        ),
    ],
)
def test_insert_request_rejects_incoherent_physical_shapes(
    request_factory: Callable[[], InsertRequest],
    error_type: type[Exception],
    message: str,
) -> None:
    """Reject declarations that cannot describe every native row exactly."""
    with pytest.raises(error_type, match=message):
        request_factory()


async def _invoke(gateway: ClickHouseGateway, operation: str) -> None:
    if operation == "query":
        await gateway.query_rows("SELECT 1")
    elif operation == "command":
        await gateway.command("CREATE TABLE example (value UInt8)")
    else:
        await gateway.insert_rows(
            InsertRequest(
                database="test_db",
                table="metadata",
                rows=((1,),),
                column_names=("value",),
                column_type_names=("UInt8",),
                settings={},
            ),
        )
