"""Public Taskiq result-backend facade."""

__all__ = ("ClickHouseResultBackend",)

from datetime import timedelta
from typing import Any, TypeVar, cast

from taskiq.abc.result_backend import AsyncResultBackend
from taskiq.abc.serializer import TaskiqSerializer
from taskiq.depends.progress_tracker import TaskProgress
from taskiq.result import TaskiqResult

from taskiq_clickhouse._backend_composition import compose_backend
from taskiq_clickhouse._backend_config import validate_backend_configuration
from taskiq_clickhouse._backend_progress import (
    get_progress as read_progress,
    set_progress as write_progress,
)
from taskiq_clickhouse._backend_results import (
    get_result as read_result,
    is_result_ready as read_result_ready,
    set_result as write_result,
)
from taskiq_clickhouse._config_input import (
    RawAuthenticationConfig,
    RawBackendConfig,
    RawEndpointConfig,
    RawStorageConfig,
)
from taskiq_clickhouse._public_error_boundary import (
    detach_public_errors,
    detach_public_errors_async,
)
from taskiq_clickhouse._types import SchemaActor, SchemaMode


_ResultT = TypeVar("_ResultT")


class ClickHouseResultBackend(AsyncResultBackend[_ResultT]):
    """ClickHouse result backend with side-effect-free validated construction."""

    @detach_public_errors
    def __init__(  # noqa: PLR0913, WPS211 - frozen public constructor.
        self,
        *,
        host: str,
        database: str,
        secure: bool,
        result_ttl: timedelta,
        purge_ttl: timedelta,
        port: int | None = None,
        username: str | None = None,
        password: str = "",
        access_token: str | None = None,
        ca_cert: str | None = None,
        client_cert: str | None = None,
        client_cert_key: str | None = None,
        server_host_name: str | None = None,
        connect_timeout: int = 10,
        send_receive_timeout: int = 300,
        namespace: str = "default",
        result_table: str = "taskiq_clickhouse_results",
        progress_table: str = "taskiq_clickhouse_progress",
        keep_results: bool = True,
        serializer: TaskiqSerializer | None = None,
        serializer_id: str | None = None,
        schema_mode: SchemaMode = "migrate",
    ) -> None:
        """Validate and retain configuration without creating process resources."""
        raw_config = RawBackendConfig(
            endpoint=RawEndpointConfig(
                host=host,
                database=database,
                secure=secure,
                port=port,
                connect_timeout=connect_timeout,
                send_receive_timeout=send_receive_timeout,
            ),
            authentication=RawAuthenticationConfig(
                username=username,
                password=password,
                access_token=access_token,
                ca_cert=ca_cert,
                client_cert=client_cert,
                client_cert_key=client_cert_key,
                server_host_name=server_host_name,
            ),
            storage=RawStorageConfig(
                result_ttl=result_ttl,
                purge_ttl=purge_ttl,
                namespace=namespace,
                result_table=result_table,
                progress_table=progress_table,
                keep_results=keep_results,
                serializer_id=serializer_id,
                schema_mode=schema_mode,
            ),
        )
        validated = validate_backend_configuration(raw_config, serializer)
        components = compose_backend(*validated)
        self._runtime = components.runtime
        self._result_codec = components.result_codec
        self._progress_codec = components.progress_codec
        self._keep_results = components.keep_results

    @detach_public_errors_async
    async def startup(self) -> None:
        """Create the process-local client and cross the readiness barrier."""
        await self._runtime.startup()

    @detach_public_errors_async
    async def shutdown(self) -> None:
        """Close the process-local client exactly once and forbid restart."""
        await self._runtime.shutdown()

    @detach_public_errors_async
    async def set_result(  # noqa: WPS110, WPS615 - inherited Taskiq API.
        self,
        task_id: str,
        result: TaskiqResult[_ResultT],  # noqa: WPS110 - inherited Taskiq API.
    ) -> None:
        """Serialize and persist one immutable result generation."""
        store = self._runtime.repository()
        await write_result(
            store,
            self._result_codec,
            task_id,
            cast("TaskiqResult[Any]", result),
        )

    @detach_public_errors_async
    async def is_result_ready(
        self,
        task_id: str,
    ) -> bool:
        """Return whether the selected latest result state is visible."""
        store = self._runtime.repository()
        return await read_result_ready(
            store,
            task_id,
        )

    @detach_public_errors_async
    async def get_result(  # noqa: WPS463, WPS615 - inherited Taskiq API.
        self,
        task_id: str,
        with_logs: bool = False,  # noqa: FBT001, FBT002 - inherited signature.
    ) -> TaskiqResult[_ResultT]:
        """Decode one latest result and optionally consume its generation."""
        store = self._runtime.repository()
        decoded = await read_result(
            store,
            self._result_codec,
            task_id,
            with_logs=with_logs,
            keep_results=self._keep_results,
        )
        return cast("TaskiqResult[_ResultT]", decoded)

    @detach_public_errors_async
    async def set_progress(  # noqa: WPS615 - inherited Taskiq API.
        self,
        task_id: str,
        progress: TaskProgress[Any],
    ) -> None:
        """Serialize and persist one immutable progress generation."""
        store = self._runtime.repository()
        await write_progress(
            store,
            self._progress_codec,
            task_id,
            progress,
        )

    @detach_public_errors_async
    async def get_progress(  # noqa: WPS463, WPS615 - inherited Taskiq API.
        self,
        task_id: str,
    ) -> TaskProgress[Any] | None:
        """Return the latest visible progress without consuming it."""
        store = self._runtime.repository()
        return await read_progress(
            store,
            self._progress_codec,
            task_id,
        )


async def _run_schema_manager(
    backend: ClickHouseResultBackend[Any],
    *,
    mode: SchemaMode,
    actor: SchemaActor,
) -> None:
    """Run a controlled barrier through the backend-owned runtime."""
    await backend._runtime.run_schema_manager(mode=mode, actor=actor)  # noqa: SLF001


def _is_new_backend(backend: ClickHouseResultBackend[Any]) -> bool | None:
    """Return a safe exact freshness observation for a trusted facade."""
    try:
        freshness = cast("object", backend._runtime.is_new)  # noqa: SLF001
    except Exception:  # noqa: BLE001 - composition details must not cross the CLI boundary.
        return None
    if freshness is not True and freshness is not False:
        return None
    return freshness
