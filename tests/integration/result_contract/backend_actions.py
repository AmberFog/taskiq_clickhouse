"""Build and instrument real public result backends for integration tests."""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, TypeVar
from unittest.mock import patch

from taskiq_clickhouse import _backend_composition as backend_composition
from taskiq_clickhouse._clickhouse.adapter import ClickHouseGateway
from taskiq_clickhouse._storage.layout import storage_layout_from_names
from taskiq_clickhouse._storage.queries import ResultQueries
from taskiq_clickhouse.backend import ClickHouseResultBackend
from tests.integration.result_contract.constants import (
    CORRUPT_LOG_PAYLOAD,
    PURGE_TTL,
    RESULT_TTL,
)
from tests.integration.result_contract.gateways import (
    CorruptLogInsertGateway,
    GatewaySwitch,
)


if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable

    from clickhouse_connect.driver.asyncclient import AsyncClient
    from taskiq.result import TaskiqResult

    from taskiq_clickhouse._clickhouse.contracts import ReadWriteGateway
    from tests.integration.settings import ClickHouseTestSettings


_RESULT_TABLE = "taskiq_clickhouse_results"
_PROGRESS_TABLE = "taskiq_clickhouse_progress"
_GatewayT = TypeVar("_GatewayT", bound="ReadWriteGateway")


@dataclass(frozen=True, slots=True)
class ResultBackendHarness:
    """Expose public backend behavior and one explicit gateway control seam."""

    facade: ClickHouseResultBackend[Any]
    gateway: GatewaySwitch
    queries: ResultQueries

    def install_gateway(
        self,
        decorator: Callable[[ReadWriteGateway], _GatewayT],
    ) -> _GatewayT:
        """Route subsequent I/O through one deterministic fault decorator."""
        return self.gateway.install(decorator)

    def reset_gateway(self) -> None:
        """Restore direct production-adapter forwarding."""
        self.gateway.reset()

    async def set_result(
        self,
        task_id: str,
        task_result: TaskiqResult[Any],
    ) -> None:
        """Delegate one result write through the public Taskiq API."""
        await self.facade.set_result(task_id, task_result)

    async def is_result_ready(self, task_id: str) -> bool:
        """Delegate one readiness check through the public Taskiq API."""
        return await self.facade.is_result_ready(task_id)

    async def get_result(
        self,
        task_id: str,
        *,
        with_logs: bool = False,
    ) -> TaskiqResult[Any]:
        """Delegate one result read through the public Taskiq API."""
        return await self.facade.get_result(task_id, with_logs=with_logs)


def _make_backend(
    settings: ClickHouseTestSettings,
    database: str,
    namespace: str,
    *,
    keep_results: bool,
) -> ClickHouseResultBackend[Any]:
    """Build one side-effect-free backend for the required ClickHouse service."""
    host = "localhost" if ":" in settings.host else settings.host
    return ClickHouseResultBackend(
        host=host,
        port=settings.port,
        username=settings.username,
        password=settings.password,
        database=database,
        secure=False,
        result_ttl=RESULT_TTL,
        purge_ttl=PURGE_TTL,
        namespace=namespace,
        result_table=_RESULT_TABLE,
        progress_table=_PROGRESS_TABLE,
        keep_results=keep_results,
    )


@asynccontextmanager
async def running_backend(
    settings: ClickHouseTestSettings,
    database: str,
    namespace: str,
    *,
    keep_results: bool,
) -> AsyncIterator[ResultBackendHarness]:
    """Start a facade with a switchable real adapter and close it terminally."""
    gateway = GatewaySwitch()
    backend = _make_backend(
        settings,
        database,
        namespace,
        keep_results=keep_results,
    )
    factory = _gateway_factory(gateway)
    try:
        with patch.object(backend_composition, "ClickHouseGateway", factory):
            await backend.startup()
        layout = storage_layout_from_names(
            database,
            _RESULT_TABLE,
            _PROGRESS_TABLE,
        )
        yield ResultBackendHarness(
            facade=backend,
            gateway=gateway,
            queries=ResultQueries(layout.result_table),
        )
    finally:
        await backend.shutdown()


async def seed_corrupt_log(
    backend: ResultBackendHarness,
    task_id: str,
    task_result: TaskiqResult[Any],
) -> None:
    """Write through the public API while corrupting only its native log cell."""
    installed = backend.install_gateway(
        lambda base: CorruptLogInsertGateway(
            delegate=base,
            corrupt_payload=CORRUPT_LOG_PAYLOAD,
        ),
    )
    try:
        await backend.set_result(task_id, task_result)
    finally:
        backend.reset_gateway()
    if not installed.corrupted:
        message = "public result write did not include a log_payload column"
        raise AssertionError(message)


def _gateway_factory(
    gateway: GatewaySwitch,
) -> Callable[[AsyncClient], GatewaySwitch]:
    """Return the exact construction callable consumed by the composition root."""

    def build(client: AsyncClient) -> GatewaySwitch:
        return gateway.bind(ClickHouseGateway(client))

    return build
