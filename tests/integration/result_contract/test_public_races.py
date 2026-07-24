"""Exercise public best-effort consume races against real ClickHouse."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from taskiq_clickhouse.exceptions import ClickHouseResultNotFoundError
from tests.integration.result_contract.assertions import assert_success_result
from tests.integration.result_contract.models import SuccessCase
from tests.integration.result_contract.race_actions import (
    run_concurrent_consume,
    run_targeted_consume,
)


if TYPE_CHECKING:
    from tests.integration.settings import ClickHouseTestSettings


pytestmark = [pytest.mark.clickhouse, pytest.mark.asyncio]


async def test_public_consume_can_return_duplicates(
    clickhouse_settings: ClickHouseTestSettings,
    clickhouse_database: str,
) -> None:
    """Prove best-effort consume permits duplicates without an exactly-once claim."""
    task_id = "duplicate-consume"
    source = SuccessCase("shared-generation", None).build()
    observed = await run_concurrent_consume(
        clickhouse_settings,
        clickhouse_database,
        task_id,
        source,
    )

    assert observed.ready_after_consume is False
    assert isinstance(observed.missing_error, ClickHouseResultNotFoundError)
    assert_success_result(observed.first, source, with_logs=False)
    assert_success_result(observed.second, source, with_logs=False)


async def test_public_tombstone_a_does_not_hide_b(
    clickhouse_settings: ClickHouseTestSettings,
    clickhouse_database: str,
) -> None:
    """Target consumption to captured A while concurrent public write B survives."""
    task_id = "targeted-consume"
    older = SuccessCase("generation-a", "log-a").build()
    newer = SuccessCase("generation-b", "log-b").build()
    observed = await run_targeted_consume(
        clickhouse_settings,
        clickhouse_database,
        task_id,
        older,
        newer,
    )

    assert observed.latest.ready is True
    assert_success_result(observed.consumed, older, with_logs=True)
    assert_success_result(observed.latest.task_result, newer, with_logs=True)
