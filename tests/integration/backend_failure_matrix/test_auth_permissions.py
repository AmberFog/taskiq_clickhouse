"""Exercise public authentication and permission failures on real ClickHouse."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import pytest
from taskiq.result import TaskiqResult

from taskiq_clickhouse.exceptions import ClickHouseBackendIOError
from tests.integration.backend_failure_matrix.access_control import (
    grant_connectivity_only,
    grant_validate_and_data_reads,
    managed_user,
    revoke_result_select,
)
from tests.integration.backend_failure_matrix.assertions import (
    assert_safe_public_error,
    log_public_error,
)
from tests.integration.backend_failure_matrix.backend_factory import (
    BackendCredentials,
    BackendScope,
    build_backend,
    endpoint_sentinels,
    preprovision_scope,
)
from tests.integration.backend_failure_matrix.constants import (
    AUTH_PASSWORD,
    AUTH_WRONG_PASSWORD,
    DENIED_TASK_ID,
    MISSING_TASK_ID,
    READ_ONLY_PASSWORD,
    REVOKED_PASSWORD,
    SCHEMA_PASSWORD,
    SECRET_LOG,
    SECRET_PAYLOAD,
    SECRET_TOKEN,
)
from tests.integration.backend_failure_matrix.gateway_probe import (
    install_storage_gateway_probe,
)
from tests.integration.backend_failure_matrix.table_io import count_result_rows


if TYPE_CHECKING:
    from clickhouse_connect.driver.asyncclient import AsyncClient

    from tests.integration.settings import ClickHouseTestSettings


pytestmark = [pytest.mark.clickhouse, pytest.mark.asyncio]


async def test_public_startup_wrong_password_is_safe_and_redacted(
    clickhouse_client: AsyncClient,
    clickhouse_settings: ClickHouseTestSettings,
    clickhouse_database: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Translate real authentication rejection without retaining connection data."""
    scope = BackendScope.unique("wrong_password")
    async with managed_user(
        clickhouse_client,
        prefix="auth_failure_user",
        password=AUTH_PASSWORD,
    ) as user:
        credentials = BackendCredentials(user.username.value, AUTH_WRONG_PASSWORD)
        backend = build_backend(
            clickhouse_settings,
            clickhouse_database,
            scope,
            credentials,
            schema_mode="validate",
        )
        try:
            caplog.clear()
            with caplog.at_level(logging.ERROR):
                with pytest.raises(ClickHouseBackendIOError) as raised:
                    await backend.startup()
                log_public_error(raised.value)

            assert_safe_public_error(
                raised.value,
                operation="backend",
                reason="client_create_failed",
                forbidden=(
                    user.username.value,
                    AUTH_PASSWORD,
                    AUTH_WRONG_PASSWORD,
                    *endpoint_sentinels(clickhouse_settings),
                ),
                log_text=caplog.text,
            )
        finally:
            await backend.shutdown()


async def test_user_without_schema_read_grants_fails_public_startup(
    clickhouse_client: AsyncClient,
    clickhouse_settings: ClickHouseTestSettings,
    clickhouse_database: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Reach physical inspection with valid auth and fail its first protected read."""
    scope = BackendScope.unique("schema_denied")
    async with managed_user(
        clickhouse_client,
        prefix="schema_denied_user",
        password=SCHEMA_PASSWORD,
    ) as user:
        await grant_connectivity_only(clickhouse_client, user)
        credentials = BackendCredentials(user.username.value, user.password)
        backend = build_backend(
            clickhouse_settings,
            clickhouse_database,
            scope,
            credentials,
            schema_mode="validate",
        )
        try:
            caplog.clear()
            with caplog.at_level(logging.ERROR):
                with pytest.raises(ClickHouseBackendIOError) as raised:
                    await backend.startup()
                log_public_error(raised.value)

            assert_safe_public_error(
                raised.value,
                operation="schema_inspection",
                reason="database_error",
                forbidden=(
                    user.username.value,
                    user.password,
                    *endpoint_sentinels(clickhouse_settings),
                ),
                log_text=caplog.text,
            )
        finally:
            await backend.shutdown()


async def test_validate_only_user_reads_but_cannot_write_result(
    clickhouse_client: AsyncClient,
    clickhouse_settings: ClickHouseTestSettings,
    clickhouse_database: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Treat real INSERT denial as definite without confirmation, retry or row."""
    scope = BackendScope.unique("write_denied")
    await preprovision_scope(clickhouse_settings, clickhouse_database, scope)
    async with managed_user(
        clickhouse_client,
        prefix="read_only_user",
        password=READ_ONLY_PASSWORD,
    ) as user:
        await grant_validate_and_data_reads(clickhouse_client, user, clickhouse_database, scope)
        credentials = BackendCredentials(user.username.value, user.password)
        backend = build_backend(
            clickhouse_settings,
            clickhouse_database,
            scope,
            credentials,
            schema_mode="validate",
        )
        try:
            await backend.startup()
            assert not await backend.is_result_ready(MISSING_TASK_ID)
            probe = install_storage_gateway_probe(backend)
            result = _private_result()

            caplog.clear()
            with caplog.at_level(logging.ERROR):
                with pytest.raises(ClickHouseBackendIOError) as raised:
                    await backend.set_result(DENIED_TASK_ID, result)
                log_public_error(raised.value)

            assert_safe_public_error(
                raised.value,
                operation="result_write",
                reason="database_error",
                forbidden=(
                    user.username.value,
                    user.password,
                    SECRET_PAYLOAD,
                    SECRET_TOKEN,
                    SECRET_LOG,
                    *endpoint_sentinels(clickhouse_settings),
                ),
                log_text=caplog.text,
            )
            assert probe.insert_calls == 1
            assert probe.confirmation_calls == 0
            assert (
                await count_result_rows(
                    clickhouse_client,
                    clickhouse_database,
                    scope,
                    DENIED_TASK_ID,
                )
                == 0
            )
        finally:
            await backend.shutdown()


async def test_revoked_select_makes_readiness_raise_instead_of_false(
    clickhouse_client: AsyncClient,
    clickhouse_settings: ClickHouseTestSettings,
    clickhouse_database: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Keep a real post-startup read denial distinct from result absence."""
    scope = BackendScope.unique("read_revoked")
    await preprovision_scope(clickhouse_settings, clickhouse_database, scope)
    async with managed_user(
        clickhouse_client,
        prefix="revoked_read_user",
        password=REVOKED_PASSWORD,
    ) as user:
        await grant_validate_and_data_reads(clickhouse_client, user, clickhouse_database, scope)
        credentials = BackendCredentials(user.username.value, user.password)
        backend = build_backend(
            clickhouse_settings,
            clickhouse_database,
            scope,
            credentials,
            schema_mode="validate",
        )
        try:
            await backend.startup()
            assert not await backend.is_result_ready(MISSING_TASK_ID)
            await revoke_result_select(clickhouse_client, user, clickhouse_database, scope)

            caplog.clear()
            with caplog.at_level(logging.ERROR):
                with pytest.raises(ClickHouseBackendIOError) as raised:
                    await backend.is_result_ready(MISSING_TASK_ID)
                log_public_error(raised.value)

            assert_safe_public_error(
                raised.value,
                operation="result_readiness",
                reason="database_error",
                forbidden=(
                    user.username.value,
                    user.password,
                    *endpoint_sentinels(clickhouse_settings),
                ),
                log_text=caplog.text,
            )
        finally:
            await backend.shutdown()


def _private_result() -> TaskiqResult[Any]:
    return TaskiqResult(
        is_err=False,
        log=SECRET_LOG,
        return_value={"payload": SECRET_PAYLOAD, "token": SECRET_TOKEN},
        execution_time=0.25,
        labels={"token": SECRET_TOKEN},
        error=None,
    )
