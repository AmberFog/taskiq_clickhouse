"""Verify finite, exactly-once ownership of client creation and cleanup."""

from __future__ import annotations

import asyncio
import traceback
from typing import TYPE_CHECKING, cast

import clickhouse_connect
import pytest

from taskiq_clickhouse import _lifecycle as lifecycle_module
from taskiq_clickhouse._client_lifecycle import OwnedClient, _OwnedTask
from taskiq_clickhouse._config_models import (
    AuthenticationConfig,
    BackendConfig,
    EndpointConfig,
    StorageConfig,
)
from taskiq_clickhouse._storage_policy import NamespaceKey, RetentionPolicy, StoragePolicy
from taskiq_clickhouse.exceptions import ClickHouseBackendIOError


if TYPE_CHECKING:
    from collections.abc import Coroutine

    from clickhouse_connect.driver.asyncclient import AsyncClient


_ASSERTION_DEADLINE_SECONDS = 1.0


class _ObservableClient:
    """Expose one optionally uncooperative raw close call."""

    def __init__(self, *, block_close: bool = False) -> None:
        self.close_calls = 0
        self.close_entered = asyncio.Event()
        self.close_cancelled = asyncio.Event()
        self.close_finished = asyncio.Event()
        self.close_release = asyncio.Event()
        if not block_close:
            self.close_release.set()

    async def close(self) -> None:
        """Ignore one task cancellation to model an uncooperative driver."""
        self.close_calls += 1
        self.close_entered.set()
        try:
            await self.close_release.wait()
        except asyncio.CancelledError:
            self.close_cancelled.set()
            await self.close_release.wait()
        self.close_finished.set()


class _LateFactory:
    """Return a client even after its owned task has been cancelled."""

    def __init__(
        self,
        client: _ObservableClient,
        *,
        error: Exception | None = None,
    ) -> None:
        self.client = client
        self.error = error
        self.calls = 0
        self.entered = asyncio.Event()
        self.cancelled = asyncio.Event()
        self.release = asyncio.Event()

    async def __call__(self) -> AsyncClient:
        """Ignore one cancellation before returning the observable client."""
        self.calls += 1
        self.entered.set()
        try:
            await self.release.wait()
        except asyncio.CancelledError:
            self.cancelled.set()
            await self.release.wait()
        if self.error is not None:
            raise self.error
        return cast("AsyncClient", self.client)


class _SynchronousCloseFailure:
    """Raise before a driver close method can return an awaitable."""

    def __init__(self, error: BaseException) -> None:
        self.error = error
        self.close_calls = 0

    def close(self) -> Coroutine[object, object, None]:
        """Fail synchronously at the raw client boundary."""
        self.close_calls += 1
        raise self.error


class _CancelledClose:
    """Expose the exact cancellation raised by one raw close call."""

    def __init__(self) -> None:
        self.cancellation = asyncio.CancelledError("driver-close-cancellation")
        self.close_calls = 0

    async def close(self) -> None:
        """Cancel the close task from inside the driver boundary."""
        self.close_calls += 1
        raise self.cancellation


class _FatalClose(BaseException):
    """Synthetic process-level close failure whose identity must survive."""


async def _raise_fatal_close(fatal: _FatalClose) -> None:
    raise fatal


async def _wait(event: asyncio.Event) -> None:
    async with asyncio.timeout(_ASSERTION_DEADLINE_SECONDS):
        await event.wait()


def _owned_client() -> OwnedClient:
    return OwnedClient("test_operation")


def _backend_config() -> BackendConfig:
    return BackendConfig(
        endpoint=EndpointConfig(
            host="clickhouse.internal",
            database="taskiq",
            secure=True,
            port=8443,
            connect_timeout=11,
            send_receive_timeout=22,
        ),
        authentication=AuthenticationConfig(
            username="worker",
            password="",
            access_token=None,
            ca_cert="ca.pem",
            client_cert="client.pem",
            client_cert_key=None,
            server_host_name="clickhouse.internal",
        ),
        storage=StorageConfig(
            policy=StoragePolicy(
                NamespaceKey("lifecycle-tests"),
                RetentionPolicy(1, 2),
            ),
            result_table="results",
            progress_table="progress",
            keep_results=True,
            serializer_id="json",
            schema_mode="validate",
        ),
    )


def _assert_safe_error(error: ClickHouseBackendIOError, reason: str) -> None:
    assert error.operation == "test_operation"
    assert error.reason == reason
    assert error.__cause__ is None
    assert error.__context__ is None


@pytest.mark.asyncio
async def test_create_client_freezes_driver_correctness_options(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pass validated connection policy and package-owned safety options once."""
    expected_client = _ObservableClient()
    received: dict[str, object] = {}

    async def get_async_client(**options: object) -> AsyncClient:
        received.update(options)
        return cast("AsyncClient", expected_client)

    monkeypatch.setattr(clickhouse_connect, "get_async_client", get_async_client)
    monkeypatch.setattr(
        lifecycle_module,
        "distribution_version",
        lambda _distribution_name: "1.2.3",
    )

    client = await lifecycle_module.create_client(_backend_config())

    assert client is cast("object", expected_client)
    assert received == {
        "host": "clickhouse.internal",
        "port": 8443,
        "username": "worker",
        "password": "",
        "access_token": None,
        "database": "taskiq",
        "interface": "https",
        "secure": True,
        "verify": True,
        "ca_cert": "ca.pem",
        "client_cert": "client.pem",
        "client_cert_key": None,
        "server_host_name": "clickhouse.internal",
        "connect_timeout": 11,
        "send_receive_timeout": 22,
        "tz_mode": "aware",
        "autogenerate_session_id": False,
        "query_retries": 2,
        "client_name": "taskiq-clickhouse/1.2.3",
    }


@pytest.mark.asyncio
async def test_successful_open_and_close_keep_one_client_owner() -> None:
    """Return the exact factory result and close it once on the success path."""
    client = _ObservableClient()

    async def client_factory() -> AsyncClient:
        return cast("AsyncClient", client)

    owned_client = _owned_client()
    opened = await owned_client.open(client_factory)
    await owned_client.close(opened)

    assert opened is cast("object", client)
    assert client.close_calls == 1


@pytest.mark.asyncio
async def test_factory_owned_cancellation_is_not_mapped_to_io_failure() -> None:
    """Keep a dependency cancellation distinct from a raw factory failure."""
    cancellation_message = "factory-cancellation"
    factory_cancellation = asyncio.CancelledError(cancellation_message)

    async def client_factory() -> AsyncClient:
        raise factory_cancellation

    with pytest.raises(asyncio.CancelledError) as raised:
        await _owned_client().open(client_factory)

    assert raised.value is factory_cancellation


@pytest.mark.asyncio
async def test_factory_failure_is_redacted_and_detached() -> None:
    """Drop raw factory details from the stable package exception."""
    secret = "password=factory-secret private.internal"  # noqa: S105  # pragma: allowlist secret

    async def client_factory() -> AsyncClient:
        raise RuntimeError(secret)

    with pytest.raises(ClickHouseBackendIOError) as raised:
        await _owned_client().open(client_factory)

    _assert_safe_error(raised.value, "client_create_failed")
    assert secret not in "".join(traceback.format_exception(raised.value))


@pytest.mark.asyncio
async def test_cancellation_cleanup_waits_for_the_owned_close_task() -> None:
    """Keep ownership until the one raw close task becomes terminal."""
    client = _ObservableClient(block_close=True)
    cleanup = asyncio.create_task(
        _owned_client().close_after_cancel(cast("AsyncClient", client)),
    )

    await _wait(client.close_entered)
    assert not cleanup.done()
    client.close_release.set()
    await cleanup
    await _wait(client.close_finished)
    assert client.close_calls == 1


@pytest.mark.asyncio
async def test_cancellation_cleanup_consumes_driver_owned_cancellation() -> None:
    """Keep a cleanup-only cancellation from escaping or triggering a retry."""
    client = _CancelledClose()

    await _owned_client().close_after_cancel(cast("AsyncClient", client))

    assert client.close_calls == 1


@pytest.mark.asyncio
async def test_cancellation_cleanup_preserves_a_terminal_fatal_signal() -> None:
    """Never subordinate a process-level close failure to caller cancellation."""
    fatal = _FatalClose()
    client = _SynchronousCloseFailure(fatal)

    with pytest.raises(_FatalClose) as raised:
        await _owned_client().close_after_cancel(cast("AsyncClient", client))

    assert raised.value is fatal
    assert client.close_calls == 1


@pytest.mark.asyncio
async def test_already_terminal_owned_task_preserves_fatal_signal() -> None:
    """Inspect a fatal task that won the terminal race before drain starts."""
    fatal = _FatalClose()
    terminal_task = asyncio.create_task(_raise_fatal_close(fatal))
    await asyncio.sleep(0)

    with pytest.raises(_FatalClose) as raised:
        await _OwnedTask(terminal_task).drain()

    assert raised.value is fatal


@pytest.mark.asyncio
async def test_failure_cleanup_preserves_a_fatal_close_signal() -> None:
    """Suppress ordinary cleanup errors only, never process-level signals."""
    fatal = _FatalClose()
    client = _SynchronousCloseFailure(fatal)

    with pytest.raises(_FatalClose) as raised:
        await _owned_client().close_after_failure(cast("AsyncClient", client))

    assert raised.value is fatal
    assert client.close_calls == 1


@pytest.mark.asyncio
async def test_factory_cancellation_keeps_first_cancellation_and_late_cleanup() -> None:
    """Ignore repeated cancellation while preserving the first caller signal."""
    client = _ObservableClient()
    factory = _LateFactory(client)
    operation = asyncio.create_task(_owned_client().open(factory))
    await _wait(factory.entered)

    operation.cancel("first-cancellation")
    await asyncio.sleep(0)
    operation.cancel("second-cancellation")
    await asyncio.sleep(0)

    assert not operation.done()
    assert not factory.cancelled.is_set()
    factory.release.set()
    with pytest.raises(asyncio.CancelledError) as raised:
        await operation

    assert raised.value.args == ("first-cancellation",)
    assert client.close_calls == 1
    assert client.close_finished.is_set()


@pytest.mark.asyncio
async def test_factory_cancellation_subordinates_late_ordinary_failure() -> None:
    """Drain one late factory failure without retrying or replacing cancellation."""
    client = _ObservableClient()
    factory = _LateFactory(client, error=RuntimeError("unsafe factory failure"))
    operation = asyncio.create_task(_owned_client().open(factory))
    await _wait(factory.entered)

    operation.cancel("outer-cancellation")
    await asyncio.sleep(0)
    assert not operation.done()
    factory.release.set()
    with pytest.raises(asyncio.CancelledError) as raised:
        await operation

    assert raised.value.args == ("outer-cancellation",)
    assert factory.calls == 1
    assert client.close_calls == 0


@pytest.mark.asyncio
async def test_close_cancellation_keeps_first_signal_and_one_close_call() -> None:
    """Drain the exact close task without replacing or retrying it."""
    client = _ObservableClient(block_close=True)
    operation = asyncio.create_task(_owned_client().close(cast("AsyncClient", client)))
    await _wait(client.close_entered)

    operation.cancel("first-cancellation")
    await asyncio.sleep(0)
    operation.cancel("second-cancellation")
    await asyncio.sleep(0)

    assert not operation.done()
    assert not client.close_cancelled.is_set()
    client.close_release.set()
    with pytest.raises(asyncio.CancelledError) as raised:
        await operation

    assert raised.value.args == ("first-cancellation",)
    assert client.close_calls == 1
    assert client.close_finished.is_set()


@pytest.mark.asyncio
async def test_close_cancellation_surfaces_delayed_fatal_signal() -> None:
    """Let a terminal fatal close result override outer cancellation exactly once."""
    fatal = _FatalClose()
    close_entered = asyncio.Event()
    close_release = asyncio.Event()
    close_calls = 0

    async def close() -> None:
        nonlocal close_calls
        close_calls += 1
        close_entered.set()
        await close_release.wait()
        raise fatal

    client = _ObservableClient()
    client.close = close  # type: ignore[method-assign]
    operation = asyncio.create_task(_owned_client().close(cast("AsyncClient", client)))
    await _wait(close_entered)

    operation.cancel("outer-cancellation")
    await asyncio.sleep(0)
    assert not operation.done()
    close_release.set()
    with pytest.raises(_FatalClose) as raised:
        await operation

    assert raised.value is fatal
    assert close_calls == 1


@pytest.mark.asyncio
async def test_synchronous_close_failure_is_redacted_and_detached() -> None:
    """Classify a raw close-call failure raised before an awaitable exists."""
    secret = "password=close-secret private.internal"  # noqa: S105  # pragma: allowlist secret
    client = _SynchronousCloseFailure(RuntimeError(secret))

    with pytest.raises(ClickHouseBackendIOError) as raised:
        await _owned_client().close(cast("AsyncClient", client))

    _assert_safe_error(raised.value, "client_close_failed")
    rendered = "".join(traceback.format_exception(raised.value))
    assert secret not in rendered
    assert client.close_calls == 1
