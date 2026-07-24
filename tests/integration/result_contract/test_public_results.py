"""Exercise complete Taskiq results through the public backend and real storage."""

from __future__ import annotations

from copy import deepcopy
from typing import TYPE_CHECKING, Any, cast

import pytest

from taskiq_clickhouse.exceptions import ClickHouseResultNotFoundError
from tests.integration.result_contract.assertions import (
    ErrorExpectation,
    assert_error_result,
    assert_success_result,
)
from tests.integration.result_contract.backend_actions import (
    running_backend,
)
from tests.integration.result_contract.gateways import VisibilityBoundaryGateway
from tests.integration.result_contract.models import (
    PublicContractError,
    SuccessCase,
    builtin_error_result,
    custom_error_result,
    unique_namespace,
)
from tests.integration.result_contract.scenario_actions import (
    run_consume_rewrite,
    run_namespace_isolation,
)


if TYPE_CHECKING:
    from tests.integration.settings import ClickHouseTestSettings


pytestmark = [pytest.mark.clickhouse, pytest.mark.asyncio]


async def test_public_success_is_frozen(
    clickhouse_settings: ClickHouseTestSettings,
    clickhouse_database: str,
) -> None:
    """Freeze one complete result without mutating or retaining its source graph."""
    source = SuccessCase("immutable", "source-log").build()
    expected = deepcopy(source)
    async with running_backend(
        clickhouse_settings,
        clickhouse_database,
        unique_namespace("public-success"),
        keep_results=True,
    ) as backend:
        await backend.set_result("complete-success", source)

        assert source == expected
        source_mapping = cast("dict[str, Any]", source.return_value)
        cast("list[Any]", source_mapping["nested"]).append("mutated")
        source.labels["queue"] = "mutated"
        source.log = "mutated-log"
        observed = await backend.get_result("complete-success", with_logs=True)

    assert_success_result(observed, expected, with_logs=True)


async def test_public_builtin_error_keeps_cause(
    clickhouse_settings: ClickHouseTestSettings,
    clickhouse_database: str,
) -> None:
    """Reconstruct a built-in task error and its cause through JSON storage."""
    source = builtin_error_result()
    async with running_backend(
        clickhouse_settings,
        clickhouse_database,
        unique_namespace("public-builtin-error"),
        keep_results=True,
    ) as backend:
        await backend.set_result("builtin-error", source)
        observed = await backend.get_result("builtin-error", with_logs=True)

    assert_error_result(
        observed,
        source,
        ErrorExpectation(
            error_type=ValueError,
            chain_attribute="__cause__",
            chain_type=RuntimeError,
            chain_message="builtin-cause",
        ),
    )


async def test_public_custom_error_keeps_context(
    clickhouse_settings: ClickHouseTestSettings,
    clickhouse_database: str,
) -> None:
    """Reconstruct an imported application exception and its context."""
    source = custom_error_result()
    async with running_backend(
        clickhouse_settings,
        clickhouse_database,
        unique_namespace("public-custom-error"),
        keep_results=True,
    ) as backend:
        await backend.set_result("custom-error", source)
        observed = await backend.get_result("custom-error", with_logs=True)

    assert_error_result(
        observed,
        source,
        ErrorExpectation(
            error_type=PublicContractError,
            chain_attribute="__context__",
            chain_type=LookupError,
            chain_message="custom-context",
        ),
    )


async def test_public_missing_and_expired_unavailable(
    clickhouse_settings: ClickHouseTestSettings,
    clickhouse_database: str,
) -> None:
    """Report both absence and exclusive-tick expiry without timing sleeps."""
    missing_task = "never-written"
    expired_task = "visibility-boundary"
    async with running_backend(
        clickhouse_settings,
        clickhouse_database,
        unique_namespace("public-unavailable"),
        keep_results=True,
    ) as backend:
        assert not await backend.is_result_ready(missing_task)
        with pytest.raises(ClickHouseResultNotFoundError, match="not_found"):
            await backend.get_result(missing_task)

        await backend.set_result(expired_task, SuccessCase("expired", None).build())
        backend.install_gateway(
            VisibilityBoundaryGateway,
        )

        assert not await backend.is_result_ready(expired_task)
        with pytest.raises(ClickHouseResultNotFoundError, match="not_found"):
            await backend.get_result(expired_task)


async def test_public_consume_allows_fresh_generation(
    clickhouse_settings: ClickHouseTestSettings,
    clickhouse_database: str,
) -> None:
    """Hide the consumed generation and expose a later public write as fresh."""
    task_id = "consume-then-rewrite"
    first = SuccessCase("first", "first-log").build()
    fresh = SuccessCase("fresh", "fresh-log").build()
    observed = await run_consume_rewrite(
        clickhouse_settings,
        clickhouse_database,
        task_id,
        first,
        fresh,
    )

    assert observed.ready_after_consume is False
    assert isinstance(observed.missing_error, ClickHouseResultNotFoundError)
    assert observed.fresh.ready is True
    assert_success_result(observed.consumed, first, with_logs=True)
    assert_success_result(observed.fresh.task_result, fresh, with_logs=True)


async def test_public_namespaces_isolate_same_task(
    clickhouse_settings: ClickHouseTestSettings,
    clickhouse_database: str,
) -> None:
    """Keep two values independent under one table pair and identical task id."""
    task_id = "shared-task-id"
    first = SuccessCase("namespace-a", "log-a").build()
    second = SuccessCase("namespace-b", "log-b").build()
    observed = await run_namespace_isolation(
        clickhouse_settings,
        clickhouse_database,
        task_id,
        first,
        second,
    )

    assert observed.first.ready is True
    assert observed.second.ready is True
    assert_success_result(observed.first.task_result, first, with_logs=True)
    assert_success_result(observed.second.task_result, second, with_logs=True)
