"""Executable client acknowledgement, retry, lifecycle, and proxy POC."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import ipaddress
import os
import time
from typing import TYPE_CHECKING, Final, cast
from unittest.mock import patch
from urllib.parse import urlsplit
from uuid import UUID, uuid4

import aiohttp
import clickhouse_connect
from clickhouse_connect.driver.asyncclient import AsyncClient
from clickhouse_connect.driver.exceptions import (
    DatabaseError,
    OperationalError,
    ProgrammingError,
    StreamFailureError,
)
from clickhouse_connect.driver.httputil import check_env_proxy


if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping
    from contextlib import AbstractContextManager
    from typing import Protocol

    from tests.integration.fixtures import ClickHouseClientFactory
    from tests.integration.settings import ClickHouseTestSettings

    class _SessionLease(Protocol):
        session: aiohttp.ClientSession

        async def wait_drained(self) -> None: ...


ACK_TABLE: Final = "poc_acknowledgement"
FAILURE_TABLE: Final = "poc_async_failure"
RETRY_TABLE: Final = "poc_response_loss"
QUERY_TIMEOUT_SECONDS: Final = 10
RECEIVE_TIMEOUT_SECONDS: Final = 1
EXPECTED_RETRY_ATTEMPTS: Final = 2
MAX_CONFIRMATION_ATTEMPTS: Final = 2
EXPECTED_STREAM_REQUEST_ATTEMPTS: Final = 1
EXPECTED_STREAM_CONTENT_READS: Final = 2
HTTP_OK: Final = 200
IPV6_TEST_PORT: Final = 8123
MIN_TRUNCATABLE_PAYLOAD_BYTES: Final = 2
POLL_INTERVAL_SECONDS: Final = 0.01
POLL_TIMEOUT_SECONDS: Final = 5
SYNC_WRITE_SETTINGS: Final = {
    "async_insert": 0,
    "wait_for_async_insert": 1,
    "wait_end_of_query": 1,
}
ACKNOWLEDGED_ASYNC_SETTINGS: Final = {
    "async_insert": 1,
    "wait_for_async_insert": 1,
    "wait_end_of_query": 1,
}
FIRE_AND_FORGET_SETTINGS: Final = {
    "async_insert": 1,
    "async_insert_busy_timeout_ms": 2_000,
    "async_insert_max_data_size": 100_000_000,
    "async_insert_max_query_number": 1_000_000,
    "async_insert_use_adaptive_busy_timeout": 0,
    "wait_for_async_insert": 0,
    "wait_end_of_query": 1,
}
ACKNOWLEDGED_FAILURE_SETTINGS: Final = {
    "async_insert": 1,
    "async_insert_busy_timeout_ms": 100,
    "async_insert_use_adaptive_busy_timeout": 0,
    "wait_for_async_insert": 1,
    "wait_end_of_query": 1,
}
CANCELLED_INSERT_SETTINGS: Final = {
    "async_insert": 1,
    "async_insert_busy_timeout_ms": 2_000,
    "async_insert_max_data_size": 100_000_000,
    "async_insert_use_adaptive_busy_timeout": 0,
    "wait_for_async_insert": 1,
    "wait_end_of_query": 1,
}
DDL_SETTINGS: Final = {"wait_end_of_query": 1}
WRONG_PASSWORD: Final = "intentionally-invalid-poc-password"  # noqa: S105  # Non-secret test input.
_ORIGINAL_AIOHTTP_REQUEST: Final = aiohttp.ClientSession.request
_ORIGINAL_STREAM_READ: Final = aiohttp.StreamReader.read

DROP_ACK_TABLE: Final = "DROP TABLE IF EXISTS poc_acknowledgement SYNC"
CREATE_ACK_TABLE: Final = """
CREATE TABLE poc_acknowledgement
(
    write_kind String,
    identity UUID,
    state UInt8,
    payload String
)
ENGINE = MergeTree
ORDER BY (write_kind, identity, state)
"""
COUNT_ACK_ROW: Final = """
SELECT count()
FROM poc_acknowledgement
WHERE write_kind = {write_kind:String}
  AND identity = {identity:UUID}
  AND state = {state:UInt8}
"""
DROP_FAILURE_TABLE: Final = "DROP TABLE IF EXISTS poc_async_failure SYNC"
CREATE_FAILURE_TABLE: Final = """
CREATE TABLE poc_async_failure
(
    identity UUID,
    value UInt8,
    CONSTRAINT only_zero CHECK value = 0
)
ENGINE = MergeTree
ORDER BY identity
"""
DROP_RETRY_TABLE: Final = "DROP TABLE IF EXISTS poc_response_loss SYNC"
CREATE_RETRY_TABLE: Final = """
CREATE TABLE poc_response_loss
(
    identity UUID,
    payload String
)
ENGINE = MergeTree
ORDER BY identity
"""
COUNT_RETRY_ROWS: Final = """
SELECT count()
FROM poc_response_loss
WHERE identity = {identity:UUID}
"""
INVALID_DDL: Final = "CREATE TABLE poc_invalid_ddl (value UInt8) ENGINE = NoSuchEngine"
DROP_LATE_DDL_TABLE: Final = "DROP TABLE IF EXISTS poc_late_ddl_failure SYNC"
LATE_DDL_FAILURE: Final = """
CREATE TABLE poc_late_ddl_failure
ENGINE = MergeTree
ORDER BY value
AS
SELECT number AS value
FROM numbers(100000)
WHERE throwIf(number = 50000, 'taskiq-clickhouse late DDL POC failure') = 0
"""
LONG_QUERY: Final = "SELECT sleep(2)"
IN_FLIGHT_QUERY: Final = "SELECT sleep(0.2), 1"
PROCESS_QUERY: Final = "SELECT count() FROM system.processes WHERE query_id = {query_id:String}"
SIMPLE_QUERY: Final = "SELECT 1"
PARTIAL_NATIVE_QUERY: Final = "SELECT number FROM numbers(2)"
STREAM_QUERY_RETRIES: Final = 2
ASYNC_INSERT_PENDING_QUERY: Final = """
SELECT count()
FROM system.asynchronous_inserts
WHERE database = currentDatabase()
  AND table = {table:String}
  AND has(entries.query_id, {query_id:String})
"""
COUNT_FAILURE_ROW: Final = """
SELECT count()
FROM poc_async_failure
WHERE identity = {identity:UUID}
"""


@dataclass(frozen=True, slots=True)
class VisibilityObservation:
    """Immediate row counts for synchronous and acknowledged async writes."""

    synchronous_count: int
    acknowledged_async_count: int


@dataclass(frozen=True, slots=True)
class ErrorObservation:
    """Observed fail-open and fail-closed insert/DDL behavior."""

    fire_and_forget_returned: bool
    fire_and_forget_was_pending: bool
    rejected_row_count: int
    synchronous_insert_error: type[BaseException]
    acknowledged_async_error: type[BaseException]
    ddl_error: type[BaseException]
    late_ddl_error: type[BaseException]


@dataclass(frozen=True, slots=True)
class ConfirmationObservation:
    """Exact confirmation results for every logical write kind."""

    confirmed_kinds: tuple[str, ...]
    absent_retry_attempts: int
    absent_retry_rows: int
    final_absent_error: type[BaseException]
    confirmation_ambiguous_error: type[BaseException]
    confirmation_definite_error: type[BaseException]
    definite_error: type[BaseException]
    ambiguous_error: type[BaseException]


@dataclass(frozen=True, slots=True)
class ResponseLossObservation:
    """Driver retry evidence after losing an already-processed response."""

    request_attempts: int
    physical_rows: int


@dataclass(frozen=True, slots=True)
class StreamFailureObservation:
    """Native response failure after successful HTTP headers."""

    request_attempts: int
    content_reads: int
    full_payload_bytes: int
    delivered_payload_bytes: int
    native_headers_seen: bool
    stream_error: type[BaseException]


@dataclass(slots=True)
class _StreamFailureCapture:
    """Own deterministic state for one partial-response injection."""

    target_session: aiohttp.ClientSession
    target_content: aiohttp.StreamReader | None = None
    request_attempts: int = 0
    content_reads: int = 0
    full_payload_bytes: int = 0
    delivered_payload_bytes: int = 0
    native_headers_seen: bool = False

    def request_hook(self) -> Callable[..., Awaitable[aiohttp.ClientResponse]]:
        async def hook(
            session: aiohttp.ClientSession,
            *args: object,
            **kwargs: object,
        ) -> aiohttp.ClientResponse:
            return await self.capture_response(session, *args, **kwargs)

        return hook

    def read_hook(self) -> Callable[..., Awaitable[bytes]]:
        async def hook(stream: aiohttp.StreamReader, amount: int = -1) -> bytes:
            return await self.truncate_native_stream(stream, amount)

        return hook

    async def capture_response(
        self,
        session: aiohttp.ClientSession,
        *args: object,
        **kwargs: object,
    ) -> aiohttp.ClientResponse:
        response = await _ORIGINAL_AIOHTTP_REQUEST(session, *args, **kwargs)  # type: ignore[arg-type]
        if session is self.target_session:
            self.request_attempts += 1
            self.target_content = response.content
            self.native_headers_seen = (
                response.status == HTTP_OK and response.headers.get("X-ClickHouse-Format") == "Native"
            )
        return response

    async def truncate_native_stream(self, stream: aiohttp.StreamReader, amount: int = -1) -> bytes:
        if stream is not self.target_content:
            return await _ORIGINAL_STREAM_READ(stream, amount)

        self.content_reads += 1
        if self.content_reads == 1:
            payload = await _ORIGINAL_STREAM_READ(stream, -1)
            self.full_payload_bytes = len(payload)
            if self.full_payload_bytes < MIN_TRUNCATABLE_PAYLOAD_BYTES:
                message = "Native POC response is too small to truncate"
                raise RuntimeError(message)
            truncated_payload = payload[:-1]
            self.delivered_payload_bytes = len(truncated_payload)
            return truncated_payload

        message = "simulated partial Native response"
        raise aiohttp.ClientPayloadError(message)


@dataclass(frozen=True, slots=True)
class LifecycleObservation:
    """Cancellation, drain, repeated close and post-close behavior."""

    cancellation_propagated: bool
    external_timeout_raised: bool
    usable_after_external_timeout: bool
    drained_result: int
    post_close_error: type[BaseException]


@dataclass(frozen=True, slots=True)
class CreationCancellationObservation:
    """Raw-driver leak behavior when factory initialization is cancelled."""

    cancellation_propagated: bool
    session_open_after_cancellation: bool
    explicit_cleanup_closed_session: bool


@dataclass(frozen=True, slots=True)
class CloseCancellationObservation:
    """Raw-driver leak behavior when close is cancelled during drain."""

    cancellation_propagated: bool
    lease_reference_lost: bool
    session_open_after_cancellation: bool
    second_close_recovered: bool


@dataclass(frozen=True, slots=True)
class InsertCancellationObservation:
    """Delivery state after cancelling an acknowledged async insert."""

    cancellation_propagated: bool
    client_remained_usable: bool
    committed_row_count: int


@dataclass(frozen=True, slots=True)
class AuthTimeoutObservation:
    """Definite authentication failure and ambiguous receive timeout."""

    auth_error: type[BaseException]
    receive_timeout_error: type[BaseException]


@dataclass(frozen=True, slots=True)
class _ConfirmationBranchObservation:
    absent_retry_attempts: int
    absent_retry_rows: int
    final_absent_error: type[BaseException]
    confirmation_ambiguous_error: type[BaseException]
    confirmation_definite_error: type[BaseException]


@dataclass(frozen=True, slots=True)
class IPv6ProxyObservation:
    """Bracketed URL and exact driver NO_PROXY behavior for IPv6."""

    normalized_host: str
    url_hostname: str | None
    url_port: int | None
    bracketed_no_proxy_bypasses: bool
    bracket_free_no_proxy_bypasses: bool


async def observe_immediate_visibility(client: AsyncClient) -> VisibilityObservation:
    """Compare immediate visibility after sync and acknowledged async inserts."""
    await _recreate(client, DROP_ACK_TABLE, CREATE_ACK_TABLE)
    sync_identity = UUID("00000000-0000-4000-8000-000000000101")
    async_identity = UUID("00000000-0000-4000-8000-000000000102")
    await _insert_ack_row(client, "result-sync", sync_identity, 0, SYNC_WRITE_SETTINGS)
    synchronous_count = await _count_ack_row(client, "result-sync", sync_identity, 0)
    await _insert_ack_row(client, "result-async", async_identity, 0, ACKNOWLEDGED_ASYNC_SETTINGS)
    acknowledged_async_count = await _count_ack_row(client, "result-async", async_identity, 0)
    return VisibilityObservation(
        synchronous_count=synchronous_count,
        acknowledged_async_count=acknowledged_async_count,
    )


async def observe_failure_acknowledgement(client: AsyncClient) -> ErrorObservation:
    """Show fire-and-forget returns before failure while acknowledged paths fail."""
    await _recreate(client, DROP_FAILURE_TABLE, CREATE_FAILURE_TABLE)
    fire_identity = UUID("00000000-0000-4000-8000-000000000111")
    acknowledged_identity = UUID("00000000-0000-4000-8000-000000000112")
    query_id = _unique_query_id("fire-and-forget")
    fire_settings: dict[str, object] = {**FIRE_AND_FORGET_SETTINGS, "query_id": query_id}
    await client.insert(
        FAILURE_TABLE,
        ((fire_identity, 1),),
        column_names=("identity", "value"),
        column_type_names=("UUID", "UInt8"),
        settings=fire_settings,
    )
    await _wait_for_async_insert_pending(client, query_id, FAILURE_TABLE)
    was_pending = True
    await _wait_for_async_insert_drain(client, query_id, FAILURE_TABLE)
    rejected_row_count = await _count_failure_row(client, fire_identity)
    synchronous_error = await _capture_database_error(
        client.insert(
            FAILURE_TABLE,
            ((UUID("00000000-0000-4000-8000-000000000113"), 1),),
            column_names=("identity", "value"),
            column_type_names=("UUID", "UInt8"),
            settings=SYNC_WRITE_SETTINGS,
        ),
    )
    acknowledged_error = await _capture_database_error(
        client.insert(
            FAILURE_TABLE,
            ((acknowledged_identity, 1),),
            column_names=("identity", "value"),
            column_type_names=("UUID", "UInt8"),
            settings=ACKNOWLEDGED_FAILURE_SETTINGS,
        ),
    )
    ddl_error = await _capture_database_error(client.command(INVALID_DDL, settings=DDL_SETTINGS))
    await client.command(DROP_LATE_DDL_TABLE, settings=DDL_SETTINGS)
    try:
        late_ddl_error = await _capture_database_error(client.command(LATE_DDL_FAILURE, settings=DDL_SETTINGS))
    finally:
        await client.command(DROP_LATE_DDL_TABLE, settings=DDL_SETTINGS)
    return ErrorObservation(
        fire_and_forget_returned=True,
        fire_and_forget_was_pending=was_pending,
        rejected_row_count=rejected_row_count,
        synchronous_insert_error=type(synchronous_error),
        acknowledged_async_error=type(acknowledged_error),
        ddl_error=type(ddl_error),
        late_ddl_error=type(late_ddl_error),
    )


async def observe_exact_confirmation_and_error_classes(client: AsyncClient) -> ConfirmationObservation:
    """Confirm every frozen identity and distinguish definite/ambiguous failures."""
    await _recreate(client, DROP_ACK_TABLE, CREATE_ACK_TABLE)
    kinds = ("result", "tombstone", "progress", "metadata")
    confirmed: list[str] = []
    for position, write_kind in enumerate(kinds, start=1):
        identity = UUID(f"00000000-0000-4000-8000-{position:012x}")
        state = 1 if write_kind == "tombstone" else 0
        await _insert_ack_row(client, write_kind, identity, state, SYNC_WRITE_SETTINGS)
        try:
            _raise_simulated_response_loss()
        except OperationalError:
            if await _count_ack_row(client, write_kind, identity, state) == 1:
                confirmed.append(write_kind)

    branches = await _exercise_confirmation_branches(client)
    definite_error = await _capture_database_error(client.command(INVALID_DDL, settings=DDL_SETTINGS))
    ambiguous_error = await _capture_operational_error(client)
    return ConfirmationObservation(
        confirmed_kinds=tuple(confirmed),
        absent_retry_attempts=branches.absent_retry_attempts,
        absent_retry_rows=branches.absent_retry_rows,
        final_absent_error=branches.final_absent_error,
        confirmation_ambiguous_error=branches.confirmation_ambiguous_error,
        confirmation_definite_error=branches.confirmation_definite_error,
        definite_error=type(definite_error),
        ambiguous_error=type(ambiguous_error),
    )


async def _exercise_confirmation_branches(client: AsyncClient) -> _ConfirmationBranchObservation:
    identity = UUID("00000000-0000-4000-8000-000000000099")
    write_attempts = 0

    async def absent_then_committed_loss() -> None:
        nonlocal write_attempts
        write_attempts += 1
        if write_attempts == 1:
            _raise_simulated_response_loss()
        await _insert_ack_row(client, "confirmation-retry", identity, 0, SYNC_WRITE_SETTINGS)
        _raise_simulated_response_loss()

    async def confirm_retry_identity() -> bool:
        return await _count_ack_row(client, "confirmation-retry", identity, 0) == 1

    await _run_confirmation_protocol(absent_then_committed_loss, confirm_retry_identity)
    absent_retry_rows = await _count_ack_row(client, "confirmation-retry", identity, 0)
    final_absent_error = await _capture_ambiguous_confirmation_error(
        _raise_ambiguous_write,
        _confirm_absent,
    )
    confirmation_ambiguous_error = await _capture_ambiguous_confirmation_error(
        _raise_ambiguous_write,
        _raise_stream_confirmation,
    )
    confirmation_definite_error = await _capture_definite_confirmation_error(
        _raise_ambiguous_write,
        _raise_definite_confirmation,
    )
    return _ConfirmationBranchObservation(
        absent_retry_attempts=write_attempts,
        absent_retry_rows=absent_retry_rows,
        final_absent_error=type(final_absent_error),
        confirmation_ambiguous_error=type(confirmation_ambiguous_error),
        confirmation_definite_error=type(confirmation_definite_error),
    )


async def observe_driver_response_loss_retry(client: AsyncClient) -> ResponseLossObservation:
    """Lose one processed insert response and observe same-body driver retry."""
    await _recreate(client, DROP_RETRY_TABLE, CREATE_RETRY_TABLE)
    identity = UUID("00000000-0000-4000-8000-000000000201")
    target_session = client._session  # noqa: SLF001  # POC inspects the exact driver retry boundary.
    if target_session is None:
        message = "initialized async client has no aiohttp session"
        raise RuntimeError(message)
    original_request = aiohttp.ClientSession.request
    attempts = 0

    async def lose_first_response(
        session: aiohttp.ClientSession,
        *args: object,
        **kwargs: object,
    ) -> aiohttp.ClientResponse:
        nonlocal attempts
        response = await original_request(session, *args, **kwargs)  # type: ignore[arg-type]
        if session is target_session:
            attempts += 1
            if attempts == 1:
                response.close()
                message = "simulated response loss"
                raise aiohttp.ServerDisconnectedError(message)
        return response

    with patch.object(aiohttp.ClientSession, "request", new=lose_first_response):
        await client.insert(
            RETRY_TABLE,
            ((identity, b"same-frozen-payload"),),
            column_names=("identity", "payload"),
            column_type_names=("UUID", "String"),
            settings=SYNC_WRITE_SETTINGS,
        )
    count_result = await client.query(COUNT_RETRY_ROWS, parameters={"identity": identity})
    return ResponseLossObservation(request_attempts=attempts, physical_rows=int(count_result.result_rows[0][0]))


async def observe_partial_native_stream_failure(client: AsyncClient) -> StreamFailureObservation:
    """Truncate one real Native response and fail its next content read."""
    target_session = client._session  # noqa: SLF001  # POC injects at the driver's response boundary.
    if target_session is None:
        message = "initialized async client has no aiohttp session"
        raise RuntimeError(message)

    capture = _StreamFailureCapture(target_session)
    original_query_retries = client.query_retries
    original_compression = client.compression
    stream_error: type[BaseException] | None = None
    client.query_retries = STREAM_QUERY_RETRIES
    client.compression = None
    try:
        with (
            patch.object(aiohttp.ClientSession, "request", new=capture.request_hook()),
            patch.object(aiohttp.StreamReader, "read", new=capture.read_hook()),
        ):
            try:
                await client.query(PARTIAL_NATIVE_QUERY)
            except StreamFailureError as error:
                stream_error = type(error)
    finally:
        client.query_retries = original_query_retries
        client.compression = original_compression

    if stream_error is None:
        message = "partial Native response did not raise StreamFailureError"
        raise AssertionError(message)

    return StreamFailureObservation(
        request_attempts=capture.request_attempts,
        content_reads=capture.content_reads,
        full_payload_bytes=capture.full_payload_bytes,
        delivered_payload_bytes=capture.delivered_payload_bytes,
        native_headers_seen=capture.native_headers_seen,
        stream_error=stream_error,
    )


async def observe_lifecycle(client_factory: ClickHouseClientFactory) -> LifecycleObservation:
    """Exercise in-flight cancellation, close drain, and read-after-close."""
    cancelled_client = await client_factory()
    observer = await client_factory()
    cancellation_query_id = _unique_query_id("cancellation")
    cancelled_task = asyncio.create_task(
        cancelled_client.query(LONG_QUERY, settings={"query_id": cancellation_query_id}),
    )
    await _wait_for_process(observer, cancellation_query_id)
    cancelled_task.cancel()
    cancellation_propagated = False
    try:
        await cancelled_task
    except asyncio.CancelledError:
        cancellation_propagated = True
    async with asyncio.timeout(QUERY_TIMEOUT_SECONDS):
        await cancelled_client.close()

    timeout_client = await client_factory()
    timeout_query_id = _unique_query_id("external-timeout")
    timeout_task = asyncio.create_task(
        timeout_client.query(LONG_QUERY, settings={"query_id": timeout_query_id}),
    )
    await _wait_for_process(observer, timeout_query_id)
    external_timeout_raised = False
    try:
        async with asyncio.timeout(0):
            await timeout_task
    except TimeoutError:
        external_timeout_raised = True
    usable_after_external_timeout = int((await timeout_client.query(SIMPLE_QUERY)).result_rows[0][0]) == 1

    draining_client = await client_factory()
    drain_query_id = _unique_query_id("drain")
    query_task = asyncio.create_task(
        draining_client.query(IN_FLIGHT_QUERY, settings={"query_id": drain_query_id}),
    )
    await _wait_for_process(observer, drain_query_id)
    close_task = asyncio.create_task(draining_client.close())
    async with asyncio.timeout(QUERY_TIMEOUT_SECONDS):
        query_result, _closed = await asyncio.gather(query_task, close_task)
    await draining_client.close()
    post_close_error = await _capture_programming_error(draining_client.query(SIMPLE_QUERY))
    return LifecycleObservation(
        cancellation_propagated=cancellation_propagated,
        external_timeout_raised=external_timeout_raised,
        usable_after_external_timeout=usable_after_external_timeout,
        drained_result=int(query_result.result_rows[0][1]),
        post_close_error=type(post_close_error),
    )


async def observe_creation_cancellation(
    settings: ClickHouseTestSettings,
) -> CreationCancellationObservation:
    """Cancel the public factory after its raw aiohttp session is allocated."""
    entered = asyncio.Event()
    release = asyncio.Event()
    captured_clients: list[AsyncClient] = []
    original_command = AsyncClient.command

    async def blocked_command(client: AsyncClient, *args: object, **kwargs: object) -> object:
        del args, kwargs
        captured_clients.append(client)
        entered.set()
        await release.wait()
        return await original_command(client, SIMPLE_QUERY)

    with patch.object(AsyncClient, "command", new=blocked_command):
        create_task = asyncio.create_task(
            clickhouse_connect.get_async_client(
                host=settings.host,
                port=settings.port,
                username=settings.username,
                password=settings.password,
                database="default",
                interface="http",
                secure=False,
            ),
        )
        cancellation_propagated = False
        try:
            async with asyncio.timeout(POLL_TIMEOUT_SECONDS):
                await entered.wait()
            cancellation_propagated = await _cancel_and_observe(create_task)
        finally:
            release.set()
            if not create_task.done():
                await _cancel_and_observe(create_task)

    client = captured_clients[0]
    session = client._session  # noqa: SLF001  # POC inspects the leaked raw resource.
    try:
        session_open = session is not None and not session.closed
        await client.close()
        cleanup_closed = session is not None and session.closed
        return CreationCancellationObservation(
            cancellation_propagated=cancellation_propagated,
            session_open_after_cancellation=session_open,
            explicit_cleanup_closed_session=cleanup_closed,
        )
    finally:
        if session is not None and not session.closed:
            await session.close()


async def observe_close_cancellation(
    client_factory: ClickHouseClientFactory,
) -> CloseCancellationObservation:
    """Cancel close during drain and prove a second raw close cannot recover."""
    client = await client_factory()
    lease = _read_session_lease(client)
    if lease is None:
        message = "initialized async client has no session lease"
        raise RuntimeError(message)
    lease_type = type(lease)
    original_wait_drained = lease_type.wait_drained
    entered = asyncio.Event()
    release = asyncio.Event()

    async def blocked_wait_drained(current_lease: _SessionLease) -> None:
        if current_lease is lease:
            entered.set()
            await release.wait()
        await original_wait_drained(current_lease)

    with patch.object(lease_type, "wait_drained", new=blocked_wait_drained):
        close_task = asyncio.create_task(client.close())
        cancellation_propagated = False
        try:
            async with asyncio.timeout(POLL_TIMEOUT_SECONDS):
                await entered.wait()
            cancellation_propagated = await _cancel_and_observe(close_task)
        finally:
            release.set()
            if not close_task.done():
                await _cancel_and_observe(close_task)

    try:
        lease_reference_lost = _read_session_lease(client) is None
        session_open = not lease.session.closed
        await client.close()
        second_close_recovered = lease.session.closed
        return CloseCancellationObservation(
            cancellation_propagated=cancellation_propagated,
            lease_reference_lost=lease_reference_lost,
            session_open_after_cancellation=session_open,
            second_close_recovered=second_close_recovered,
        )
    finally:
        if not lease.session.closed:
            await lease.session.close()


def _read_session_lease(client: AsyncClient) -> _SessionLease | None:
    try:
        owner = object.__getattribute__(client, "_backend")
        attribute = "session_lease"
    except AttributeError:
        owner = client
        attribute = "_session_lease"
    return cast("_SessionLease | None", object.__getattribute__(owner, attribute))


async def observe_acknowledged_insert_cancellation(client: AsyncClient) -> InsertCancellationObservation:
    """Cancel a queued acknowledged insert and observe its later commit."""
    await _recreate(client, DROP_ACK_TABLE, CREATE_ACK_TABLE)
    identity = UUID("00000000-0000-4000-8000-000000000121")
    query_id = _unique_query_id("cancelled-insert")
    settings: dict[str, object] = {**CANCELLED_INSERT_SETTINGS, "query_id": query_id}
    insert_task = asyncio.create_task(
        _insert_ack_row(client, "cancelled", identity, 0, settings),
    )
    await _wait_for_async_insert_pending(client, query_id, ACK_TABLE)
    insert_task.cancel()
    cancellation_propagated = False
    try:
        await insert_task
    except asyncio.CancelledError:
        cancellation_propagated = True
    client_remained_usable = int((await client.query(SIMPLE_QUERY)).result_rows[0][0]) == 1
    await _wait_for_async_insert_drain(client, query_id, ACK_TABLE)
    committed_row_count = await _wait_for_ack_row_count(client, "cancelled", identity, 0)
    return InsertCancellationObservation(
        cancellation_propagated=cancellation_propagated,
        client_remained_usable=client_remained_usable,
        committed_row_count=committed_row_count,
    )


async def observe_auth_and_receive_timeout(
    settings: ClickHouseTestSettings,
    database: str,
) -> AuthTimeoutObservation:
    """Classify bad credentials as definite and a receive timeout as I/O."""
    auth_error = await _capture_database_error(
        clickhouse_connect.get_async_client(
            host=settings.host,
            port=settings.port,
            username=settings.username,
            password=WRONG_PASSWORD,
            database=database,
            interface="http",
            secure=False,
        ),
    )
    timeout_client = await clickhouse_connect.get_async_client(
        host=settings.host,
        port=settings.port,
        username=settings.username,
        password=settings.password,
        database=database,
        interface="http",
        secure=False,
        query_retries=0,
        send_receive_timeout=RECEIVE_TIMEOUT_SECONDS,
    )
    try:
        try:
            await timeout_client.query(LONG_QUERY)
        except OperationalError as error:
            receive_timeout_error = error
        else:
            message = "receive-timeout query unexpectedly succeeded"
            raise AssertionError(message)
    finally:
        await timeout_client.close()
    return AuthTimeoutObservation(
        auth_error=type(auth_error),
        receive_timeout_error=type(receive_timeout_error),
    )


async def observe_ipv6_proxy_contract() -> IPv6ProxyObservation:
    """Record why literal IPv6 cannot have a standard deterministic proxy contract."""
    address = ipaddress.ip_address("2001:db8::1")
    normalized_host = f"[{address.compressed}]"
    parsed_url = urlsplit(f"http://{normalized_host}:{IPV6_TEST_PORT}")
    proxy = "http://127.0.0.1:65534"
    with _proxy_environment(proxy, normalized_host):
        bracketed_result = check_env_proxy("http", normalized_host, IPV6_TEST_PORT)
    with _proxy_environment(proxy, address.compressed):
        bracket_free_result = check_env_proxy("http", normalized_host, IPV6_TEST_PORT)
    client = AsyncClient(interface="http", host=normalized_host, port=IPV6_TEST_PORT)
    try:
        if client.url != f"http://{normalized_host}:{IPV6_TEST_PORT}":
            message = "normalized IPv6 host produced an invalid driver URL"
            raise RuntimeError(message)
    finally:
        await client.close()
    return IPv6ProxyObservation(
        normalized_host=normalized_host,
        url_hostname=parsed_url.hostname,
        url_port=parsed_url.port,
        bracketed_no_proxy_bypasses=bracketed_result is None,
        bracket_free_no_proxy_bypasses=bracket_free_result is None,
    )


async def _insert_ack_row(
    client: AsyncClient,
    write_kind: str,
    identity: UUID,
    state: int,
    settings: Mapping[str, object],
) -> None:
    await client.insert(
        ACK_TABLE,
        ((write_kind, identity, state, b"payload"),),
        column_names=("write_kind", "identity", "state", "payload"),
        column_type_names=("String", "UUID", "UInt8", "String"),
        settings=dict(settings),
    )


async def _count_ack_row(client: AsyncClient, write_kind: str, identity: UUID, state: int) -> int:
    result = await client.query(
        COUNT_ACK_ROW,
        parameters={"write_kind": write_kind, "identity": identity, "state": state},
    )
    return int(result.result_rows[0][0])


async def _capture_database_error(awaitable: Awaitable[object]) -> DatabaseError:
    try:
        await awaitable
    except DatabaseError as error:
        return error
    message = "operation unexpectedly succeeded"
    raise AssertionError(message)


async def _capture_operational_error(client: AsyncClient) -> OperationalError:
    target_session = client._session  # noqa: SLF001  # POC inspects driver classification.
    original_request = aiohttp.ClientSession.request

    async def fail_request(
        session: aiohttp.ClientSession,
        *args: object,
        **kwargs: object,
    ) -> aiohttp.ClientResponse:
        if session is target_session:
            message = "simulated timeout before response"
            raise aiohttp.ServerTimeoutError(message)
        return await original_request(session, *args, **kwargs)  # type: ignore[arg-type]

    try:
        with patch.object(aiohttp.ClientSession, "request", new=fail_request):
            await client.query(SIMPLE_QUERY)
    except OperationalError as error:
        return error
    message = "network fault was not classified as OperationalError"
    raise AssertionError(message)


async def _capture_programming_error(awaitable: Awaitable[object]) -> ProgrammingError:
    try:
        await awaitable
    except ProgrammingError as error:
        return error
    message = "closed client operation unexpectedly succeeded"
    raise AssertionError(message)


async def _wait_for_process(client: AsyncClient, query_id: str) -> None:
    deadline = time.monotonic() + POLL_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        result = await client.query(PROCESS_QUERY, parameters={"query_id": query_id})
        if int(result.result_rows[0][0]) == 1:
            return
        await asyncio.sleep(POLL_INTERVAL_SECONDS)
    message = f"query {query_id!r} did not become observable"
    raise TimeoutError(message)


async def _wait_for_async_insert_pending(client: AsyncClient, query_id: str, table: str) -> None:
    deadline = time.monotonic() + POLL_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if await _async_insert_pending(client, query_id, table) > 0:
            return
        await asyncio.sleep(POLL_INTERVAL_SECONDS)
    message = f"async insert {query_id!r} never became pending"
    raise TimeoutError(message)


async def _wait_for_async_insert_drain(client: AsyncClient, query_id: str, table: str) -> None:
    deadline = time.monotonic() + POLL_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if await _async_insert_pending(client, query_id, table) == 0:
            return
        await asyncio.sleep(POLL_INTERVAL_SECONDS)
    message = f"async insert {query_id!r} did not drain"
    raise TimeoutError(message)


async def _async_insert_pending(client: AsyncClient, query_id: str, table: str) -> int:
    result = await client.query(
        ASYNC_INSERT_PENDING_QUERY,
        parameters={"query_id": query_id, "table": table},
    )
    return int(result.result_rows[0][0])


async def _count_failure_row(client: AsyncClient, identity: UUID) -> int:
    result = await client.query(COUNT_FAILURE_ROW, parameters={"identity": identity})
    return int(result.result_rows[0][0])


async def _wait_for_ack_row_count(
    client: AsyncClient,
    write_kind: str,
    identity: UUID,
    state: int,
) -> int:
    deadline = time.monotonic() + POLL_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        count = await _count_ack_row(client, write_kind, identity, state)
        if count > 0:
            return count
        await asyncio.sleep(POLL_INTERVAL_SECONDS)
    message = f"acknowledged row {identity} never became visible"
    raise TimeoutError(message)


async def _recreate(client: AsyncClient, drop_query: str, create_query: str) -> None:
    await client.command(drop_query, settings=DDL_SETTINGS)
    await client.command(create_query, settings=DDL_SETTINGS)


def _proxy_environment(proxy: str, no_proxy: str) -> AbstractContextManager[dict[str, str]]:
    environment = {
        "http_proxy": proxy,
        "HTTP_PROXY": proxy,
        "no_proxy": no_proxy,
        "NO_PROXY": no_proxy,
    }
    return patch.dict(os.environ, environment, clear=False)


def _raise_simulated_response_loss() -> None:
    message = "simulated response loss after the server processed the row"
    raise OperationalError(message)


async def _run_confirmation_protocol(
    write: Callable[[], Awaitable[None]],
    confirm: Callable[[], Awaitable[bool]],
) -> None:
    for attempt in range(MAX_CONFIRMATION_ATTEMPTS):
        try:
            await write()
        except (OperationalError, StreamFailureError):
            if await confirm():
                return
            if attempt + 1 == MAX_CONFIRMATION_ATTEMPTS:
                raise
        else:
            return


async def _raise_ambiguous_write() -> None:
    _raise_simulated_response_loss()


async def _confirm_absent() -> bool:
    return False


async def _raise_stream_confirmation() -> bool:
    message = "simulated partial confirmation response"
    raise StreamFailureError(message)


async def _raise_definite_confirmation() -> bool:
    message = "simulated definite confirmation query failure"
    raise DatabaseError(message)


async def _capture_ambiguous_confirmation_error(
    write: Callable[[], Awaitable[None]],
    confirm: Callable[[], Awaitable[bool]],
) -> OperationalError | StreamFailureError:
    try:
        await _run_confirmation_protocol(write, confirm)
    except (OperationalError, StreamFailureError) as error:
        return error
    message = "confirmation protocol unexpectedly succeeded"
    raise AssertionError(message)


async def _capture_definite_confirmation_error(
    write: Callable[[], Awaitable[None]],
    confirm: Callable[[], Awaitable[bool]],
) -> DatabaseError:
    try:
        await _run_confirmation_protocol(write, confirm)
    except DatabaseError as error:
        return error
    message = "confirmation protocol did not preserve definite failure"
    raise AssertionError(message)


def _unique_query_id(label: str) -> str:
    return f"taskiq-clickhouse-poc-{label}-{uuid4().hex}"


async def _cancel_and_observe(task: asyncio.Task[object]) -> bool:
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        return True
    return False
