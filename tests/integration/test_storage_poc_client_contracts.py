"""Validate client acknowledgement, retry, lifecycle, and IPv6 premises."""

from clickhouse_connect.driver.asyncclient import AsyncClient
from clickhouse_connect.driver.exceptions import (
    DatabaseError,
    OperationalError,
    ProgrammingError,
    StreamFailureError,
)
import pytest

from tests.integration.evidence import write_evidence
from tests.integration.fixtures import ClickHouseClientFactory
from tests.integration.poc_client_contracts import (
    EXPECTED_RETRY_ATTEMPTS,
    EXPECTED_STREAM_CONTENT_READS,
    EXPECTED_STREAM_REQUEST_ATTEMPTS,
    IPV6_TEST_PORT,
    observe_acknowledged_insert_cancellation,
    observe_auth_and_receive_timeout,
    observe_close_cancellation,
    observe_creation_cancellation,
    observe_driver_response_loss_retry,
    observe_exact_confirmation_and_error_classes,
    observe_failure_acknowledgement,
    observe_immediate_visibility,
    observe_ipv6_proxy_contract,
    observe_lifecycle,
    observe_partial_native_stream_failure,
)
from tests.integration.settings import ClickHouseTestSettings


pytestmark = [pytest.mark.clickhouse, pytest.mark.asyncio]


async def test_sync_and_acknowledged_async_writes_are_immediately_visible(
    clickhouse_client: AsyncClient,
    clickhouse_settings: ClickHouseTestSettings,
) -> None:
    """Require read-after-ack visibility for both admissible write modes."""
    observation = await observe_immediate_visibility(clickhouse_client)
    await write_evidence(
        clickhouse_settings,
        "poc-client-immediate-visibility.json",
        {
            "acknowledged_async_count": observation.acknowledged_async_count,
            "synchronous_count": observation.synchronous_count,
        },
    )

    assert observation.synchronous_count == 1
    assert observation.acknowledged_async_count == 1


async def test_fire_and_forget_hides_failure_while_acknowledged_paths_surface_it(
    clickhouse_client: AsyncClient,
    clickhouse_settings: ClickHouseTestSettings,
) -> None:
    """Reject wait_for_async_insert=0 and require DDL/insert failures."""
    observation = await observe_failure_acknowledgement(clickhouse_client)
    await write_evidence(
        clickhouse_settings,
        "poc-client-acknowledgement-failures.json",
        {
            "acknowledged_async_error": _error_name(observation.acknowledged_async_error),
            "ddl_error": _error_name(observation.ddl_error),
            "fire_and_forget_returned": observation.fire_and_forget_returned,
            "fire_and_forget_was_pending": observation.fire_and_forget_was_pending,
            "late_ddl_error": _error_name(observation.late_ddl_error),
            "rejected_row_count": observation.rejected_row_count,
            "synchronous_insert_error": _error_name(observation.synchronous_insert_error),
        },
    )

    assert observation.fire_and_forget_returned
    assert observation.fire_and_forget_was_pending
    assert observation.rejected_row_count == 0
    assert issubclass(observation.synchronous_insert_error, DatabaseError)
    assert not issubclass(observation.synchronous_insert_error, OperationalError)
    assert issubclass(observation.acknowledged_async_error, DatabaseError)
    assert not issubclass(observation.acknowledged_async_error, OperationalError)
    assert issubclass(observation.ddl_error, DatabaseError)
    assert not issubclass(observation.ddl_error, OperationalError)
    assert issubclass(observation.late_ddl_error, DatabaseError)
    assert not issubclass(observation.late_ddl_error, OperationalError)


async def test_exact_confirmation_and_error_classifier_cover_every_write_kind(
    clickhouse_client: AsyncClient,
    clickhouse_settings: ClickHouseTestSettings,
) -> None:
    """Confirm frozen identities and separate definite from ambiguous errors."""
    observation = await observe_exact_confirmation_and_error_classes(clickhouse_client)
    await write_evidence(
        clickhouse_settings,
        "poc-client-confirmation.json",
        {
            "absent_retry_attempts": observation.absent_retry_attempts,
            "absent_retry_rows": observation.absent_retry_rows,
            "ambiguous_error": _error_name(observation.ambiguous_error),
            "confirmation_ambiguous_error": _error_name(observation.confirmation_ambiguous_error),
            "confirmation_definite_error": _error_name(observation.confirmation_definite_error),
            "confirmed_kinds": observation.confirmed_kinds,
            "definite_error": _error_name(observation.definite_error),
            "final_absent_error": _error_name(observation.final_absent_error),
        },
    )

    assert observation.confirmed_kinds == ("result", "tombstone", "progress", "metadata")
    assert observation.absent_retry_attempts == EXPECTED_RETRY_ATTEMPTS
    assert observation.absent_retry_rows == 1
    assert observation.final_absent_error is OperationalError
    assert observation.confirmation_ambiguous_error is StreamFailureError
    assert observation.confirmation_definite_error is DatabaseError
    assert issubclass(observation.definite_error, DatabaseError)
    assert not issubclass(observation.definite_error, OperationalError)
    assert issubclass(observation.ambiguous_error, OperationalError)


async def test_driver_retry_can_duplicate_one_frozen_insert_after_response_loss(
    clickhouse_client: AsyncClient,
    clickhouse_settings: ClickHouseTestSettings,
) -> None:
    """Prove confirmation must tolerate physical duplicates of one identity."""
    observation = await observe_driver_response_loss_retry(clickhouse_client)
    await write_evidence(
        clickhouse_settings,
        "poc-client-response-loss.json",
        {
            "physical_rows": observation.physical_rows,
            "request_attempts": observation.request_attempts,
        },
    )

    assert observation.request_attempts == EXPECTED_RETRY_ATTEMPTS
    assert observation.physical_rows == EXPECTED_RETRY_ATTEMPTS


async def test_partial_native_stream_failure_is_ambiguous_and_not_retried(
    clickhouse_client: AsyncClient,
    clickhouse_settings: ClickHouseTestSettings,
) -> None:
    """Classify a post-header stream break explicitly without driver retry."""
    observation = await observe_partial_native_stream_failure(clickhouse_client)
    await write_evidence(
        clickhouse_settings,
        "poc-client-stream-failure.json",
        {
            "content_reads": observation.content_reads,
            "delivered_payload_bytes": observation.delivered_payload_bytes,
            "full_payload_bytes": observation.full_payload_bytes,
            "native_headers_seen": observation.native_headers_seen,
            "request_attempts": observation.request_attempts,
            "stream_error": _error_name(observation.stream_error),
        },
    )

    assert observation.native_headers_seen
    assert observation.full_payload_bytes > 1
    assert observation.delivered_payload_bytes == observation.full_payload_bytes - 1
    assert observation.content_reads == EXPECTED_STREAM_CONTENT_READS
    assert observation.request_attempts == EXPECTED_STREAM_REQUEST_ATTEMPTS
    assert observation.stream_error is StreamFailureError
    assert not issubclass(observation.stream_error, DatabaseError)
    assert not issubclass(observation.stream_error, OperationalError)


async def test_client_cancellation_close_drain_and_post_close_are_explicit(
    clickhouse_client_factory: ClickHouseClientFactory,
    clickhouse_settings: ClickHouseTestSettings,
) -> None:
    """Propagate cancellation, drain in-flight work and reject reads after close."""
    observation = await observe_lifecycle(clickhouse_client_factory)
    await write_evidence(
        clickhouse_settings,
        "poc-client-lifecycle.json",
        {
            "cancellation_propagated": observation.cancellation_propagated,
            "drained_result": observation.drained_result,
            "external_timeout_raised": observation.external_timeout_raised,
            "post_close_error": _error_name(observation.post_close_error),
            "usable_after_external_timeout": observation.usable_after_external_timeout,
        },
    )

    assert observation.cancellation_propagated
    assert observation.external_timeout_raised
    assert observation.usable_after_external_timeout
    assert observation.drained_result == 1
    assert issubclass(observation.post_close_error, ProgrammingError)


async def test_factory_cancellation_leak_requires_package_owned_cleanup(
    clickhouse_settings: ClickHouseTestSettings,
) -> None:
    """Record why startup must shield the raw driver factory task."""
    observation = await observe_creation_cancellation(clickhouse_settings)
    await write_evidence(
        clickhouse_settings,
        "poc-client-creation-cancellation.json",
        {
            "cancellation_propagated": observation.cancellation_propagated,
            "explicit_cleanup_closed_session": observation.explicit_cleanup_closed_session,
            "session_open_after_cancellation": observation.session_open_after_cancellation,
        },
    )

    assert observation.cancellation_propagated
    assert observation.session_open_after_cancellation
    assert observation.explicit_cleanup_closed_session


async def test_close_cancellation_loses_raw_lease_and_cannot_be_retried(
    clickhouse_client_factory: ClickHouseClientFactory,
    clickhouse_settings: ClickHouseTestSettings,
) -> None:
    """Record why package shutdown must shield client close to completion."""
    observation = await observe_close_cancellation(clickhouse_client_factory)
    await write_evidence(
        clickhouse_settings,
        "poc-client-close-cancellation.json",
        {
            "cancellation_propagated": observation.cancellation_propagated,
            "lease_reference_lost": observation.lease_reference_lost,
            "second_close_recovered": observation.second_close_recovered,
            "session_open_after_cancellation": observation.session_open_after_cancellation,
        },
    )

    assert observation.cancellation_propagated
    assert observation.lease_reference_lost
    assert observation.session_open_after_cancellation
    assert not observation.second_close_recovered


async def test_cancelled_acknowledged_insert_has_ambiguous_delivery(
    clickhouse_client: AsyncClient,
    clickhouse_settings: ClickHouseTestSettings,
) -> None:
    """Propagate cancellation without retry even though the queued row commits."""
    observation = await observe_acknowledged_insert_cancellation(clickhouse_client)
    await write_evidence(
        clickhouse_settings,
        "poc-client-insert-cancellation.json",
        {
            "cancellation_propagated": observation.cancellation_propagated,
            "client_remained_usable": observation.client_remained_usable,
            "committed_row_count": observation.committed_row_count,
        },
    )

    assert observation.cancellation_propagated
    assert observation.client_remained_usable
    assert observation.committed_row_count == 1


async def test_authentication_is_definite_and_receive_timeout_is_ambiguous(
    clickhouse_settings: ClickHouseTestSettings,
    clickhouse_database: str,
) -> None:
    """Keep credential rejection distinct from transport timeout."""
    observation = await observe_auth_and_receive_timeout(clickhouse_settings, clickhouse_database)
    await write_evidence(
        clickhouse_settings,
        "poc-client-auth-timeout.json",
        {
            "auth_error": _error_name(observation.auth_error),
            "receive_timeout_error": _error_name(observation.receive_timeout_error),
        },
    )

    assert issubclass(observation.auth_error, DatabaseError)
    assert not issubclass(observation.auth_error, OperationalError)
    assert issubclass(observation.receive_timeout_error, OperationalError)


async def test_literal_ipv6_is_rejected_by_v0_1_proxy_contract(
    clickhouse_settings: ClickHouseTestSettings,
) -> None:
    """Show the driver's bracketed host misses a standard IPv6 NO_PROXY entry."""
    observation = await observe_ipv6_proxy_contract()
    await write_evidence(
        clickhouse_settings,
        "poc-client-ipv6-proxy.json",
        {
            "bracket_free_no_proxy_bypasses": observation.bracket_free_no_proxy_bypasses,
            "bracketed_no_proxy_bypasses": observation.bracketed_no_proxy_bypasses,
            "normalized_host": observation.normalized_host,
            "url_hostname": observation.url_hostname,
            "url_port": observation.url_port,
        },
    )

    assert observation.normalized_host == "[2001:db8::1]"
    assert observation.url_hostname == "2001:db8::1"
    assert observation.url_port == IPV6_TEST_PORT
    assert observation.bracketed_no_proxy_bypasses
    assert not observation.bracket_free_no_proxy_bypasses


def _error_name(error_type: type[BaseException]) -> str:
    return f"{error_type.__module__}.{error_type.__qualname__}"
