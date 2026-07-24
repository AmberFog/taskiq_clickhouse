"""Shared client construction and schema-operation error policy."""

from __future__ import annotations

import asyncio
from importlib.metadata import version as distribution_version
from typing import TYPE_CHECKING, Protocol

import clickhouse_connect

from taskiq_clickhouse.exceptions import (
    ClickHouseBackendIOError,
    ClickHouseLifecycleError,
    ClickHouseResultBackendError,
    rebuild_public_error,
)


if TYPE_CHECKING:
    from clickhouse_connect.driver.asyncclient import AsyncClient

    from taskiq_clickhouse._config_models import BackendConfig
    from taskiq_clickhouse._types import SchemaActor, SchemaMode


_SCHEMA_MANAGER_OPERATION = "schema_manager"


class SchemaBarrier(Protocol):
    async def __call__(
        self,
        client: AsyncClient,
        *,
        mode: SchemaMode,
        actor: SchemaActor,
    ) -> None:
        """Cross the complete migration, schema and namespace barrier."""
        ...


class SchemaOperation:
    _operation_failed = "operation_failed"
    _backend_not_new = "backend_not_new"

    def __init__(self, operation: str = _SCHEMA_MANAGER_OPERATION) -> None:
        """Bind safe errors to the lifecycle operation that owns the client."""
        self._operation = operation

    async def capture(
        self,
        runner: SchemaBarrier,
        client: AsyncClient,
        *,
        mode: SchemaMode,
        actor: SchemaActor,
    ) -> BaseException | None:
        """Capture raw errors only until client cleanup has completed."""
        try:
            await runner(
                client,
                mode=mode,
                actor=actor,
            )
        except asyncio.CancelledError:
            raise
        except BaseException as error:  # noqa: BLE001 - cleanup also covers fatal failures.
            return error
        return None

    def translate_error(self, error: BaseException) -> BaseException:
        """Detach public errors from unsafe implementation tracebacks."""
        if isinstance(error, ClickHouseResultBackendError):
            return rebuild_public_error(error)
        if isinstance(error, Exception):
            return self.backend_error(self._operation_failed)
        return error

    def lifecycle_error(self, reason: str) -> ClickHouseLifecycleError:
        """Build a code-only lifecycle error."""
        return ClickHouseLifecycleError(self._operation, reason)

    def backend_error(self, reason: str) -> ClickHouseBackendIOError:
        """Build a code-only backend I/O error."""
        return ClickHouseBackendIOError(self._operation, reason)

    def backend_not_new_error(self) -> ClickHouseLifecycleError:
        """Build the lifecycle error for an unavailable backend lease."""
        return self.lifecycle_error(self._backend_not_new)


async def create_client(config: BackendConfig) -> AsyncClient:
    """Create one package-owned client with frozen correctness options."""
    endpoint = config.endpoint
    authentication = config.authentication
    return await clickhouse_connect.get_async_client(
        host=endpoint.host,
        port=endpoint.port,
        username=authentication.username,
        password=authentication.password,
        access_token=authentication.access_token,
        database=endpoint.database,
        interface="https" if endpoint.secure else "http",
        secure=endpoint.secure,
        verify=True,
        ca_cert=authentication.ca_cert,
        client_cert=authentication.client_cert,
        client_cert_key=authentication.client_cert_key,
        server_host_name=authentication.server_host_name,
        connect_timeout=endpoint.connect_timeout,
        send_receive_timeout=endpoint.send_receive_timeout,
        tz_mode="aware",
        autogenerate_session_id=False,
        query_retries=2,
        client_name=f"taskiq-clickhouse/{distribution_version('taskiq-clickhouse')}",
    )
