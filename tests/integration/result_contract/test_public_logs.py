"""Exercise split-log behavior through the public backend and real storage."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from taskiq_clickhouse.exceptions import ClickHouseDecodeError
from tests.integration.result_contract.assertions import assert_success_result
from tests.integration.result_contract.backend_actions import (
    running_backend,
    seed_corrupt_log,
)
from tests.integration.result_contract.constants import THREE_MIB
from tests.integration.result_contract.models import SuccessCase, unique_namespace


if TYPE_CHECKING:
    from tests.integration.settings import ClickHouseTestSettings


pytestmark = [pytest.mark.clickhouse, pytest.mark.asyncio]


@pytest.mark.parametrize(
    "task_log",
    [
        pytest.param(None, id="none"),
        pytest.param("", id="empty"),
        pytest.param("L" * THREE_MIB, id="three-mib"),
    ],
)
async def test_public_log_shapes_round_trip(
    task_log: str | None,
    clickhouse_settings: ClickHouseTestSettings,
    clickhouse_database: str,
) -> None:
    """Preserve each supported log edge through its independent column."""
    source = SuccessCase("log-shape", task_log).build()
    async with running_backend(
        clickhouse_settings,
        clickhouse_database,
        unique_namespace("public-log"),
        keep_results=True,
    ) as backend:
        await backend.set_result("log-shape", source)
        observed = await backend.get_result("log-shape", with_logs=True)

    assert_success_result(observed, source, with_logs=True)


async def test_public_result_log_generation_is_atomic(
    clickhouse_settings: ClickHouseTestSettings,
    clickhouse_database: str,
) -> None:
    """Never combine a newer result blob with an older generation's log."""
    task_id = "generation-pair"
    older = SuccessCase("older-result", "older-log").build()
    newer = SuccessCase("newer-result", "newer-log").build()
    async with running_backend(
        clickhouse_settings,
        clickhouse_database,
        unique_namespace("public-generation-pair"),
        keep_results=True,
    ) as backend:
        await backend.set_result(task_id, older)
        await backend.set_result(task_id, newer)
        observed = await backend.get_result(task_id, with_logs=True)

    assert_success_result(observed, newer, with_logs=True)


async def test_public_corrupt_log_can_be_omitted(
    clickhouse_settings: ClickHouseTestSettings,
    clickhouse_database: str,
) -> None:
    """Leave corrupt log state unconsumed, then consume without reading that blob."""
    task_id = "corrupt-log"
    source = SuccessCase("healthy-result", "unavailable-log").build()
    async with running_backend(
        clickhouse_settings,
        clickhouse_database,
        unique_namespace("public-corrupt-log"),
        keep_results=False,
    ) as backend:
        await seed_corrupt_log(backend, task_id, source)

        with pytest.raises(ClickHouseDecodeError, match="log_payload_decode_failed"):
            await backend.get_result(task_id, with_logs=True)
        assert await backend.is_result_ready(task_id)

        observed = await backend.get_result(task_id, with_logs=False)
        assert not await backend.is_result_ready(task_id)

    assert_success_result(observed, source, with_logs=False)
