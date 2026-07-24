"""Prove opaque bytes and server-owned microsecond time on real ClickHouse."""

from datetime import timedelta

from clickhouse_connect.driver.asyncclient import AsyncClient
import pytest

from tests.integration.poc_bytes_time import (
    BYTE_CASE_ROWS,
    DATETIME64_MAX,
    MICROSECOND,
    PURGE_TTL,
    RESULT_TTL,
    SIMULATED_ROLLBACK,
    TIME_CASE_ROWS,
    TIME_PRECISION_VALUE,
    exercise_bytes_roundtrip,
    exercise_datetime64,
    exercise_generation_allocator,
    write_bytes_evidence,
    write_generation_evidence,
    write_time_evidence,
)
from tests.integration.settings import ClickHouseTestSettings


pytestmark = [pytest.mark.clickhouse, pytest.mark.asyncio]


async def test_string_payloads_are_lossless_bytes_through_direct_and_aliased_projections(
    clickhouse_client: AsyncClient,
    clickhouse_settings: ClickHouseTestSettings,
) -> None:
    """Preserve every byte case without UTF-8 decoding or base64."""
    observation = await exercise_bytes_roundtrip(clickhouse_client)
    await write_bytes_evidence(clickhouse_settings, observation)

    assert observation.direct_rows == BYTE_CASE_ROWS
    assert observation.aliased_rows == BYTE_CASE_ROWS
    assert all(isinstance(payload, bytes) for _case_id, payload in observation.direct_rows)
    assert len(BYTE_CASE_ROWS[-1][1]) >= 3 * 1024**2


async def test_datetime64_is_aware_exact_and_exclusive_at_microsecond_boundary(
    clickhouse_client: AsyncClient,
    clickhouse_settings: ClickHouseTestSettings,
) -> None:
    """Round-trip conservative range limits and typed microsecond binds."""
    observation = await exercise_datetime64(clickhouse_client)
    await write_time_evidence(clickhouse_settings, observation)

    assert observation.rows == TIME_CASE_ROWS
    assert all(value.utcoffset() == timedelta(0) for _case_id, value in observation.rows)
    assert observation.rows[1][1] == TIME_PRECISION_VALUE
    assert observation.exact_match
    assert observation.visible_before
    assert not observation.visible_at_equality
    assert not observation.visible_after
    assert observation.out_of_range_bind_rejected
    assert observation.out_of_range_insert_rejected
    assert observation.last_valid_visible_until < observation.last_valid_purge_at
    assert observation.last_valid_purge_at == DATETIME64_MAX
    assert observation.deadline_overflow_rejected


async def test_generation_and_deadlines_use_server_time_during_simulated_rollback(
    clickhouse_client: AsyncClient,
    clickhouse_settings: ClickHouseTestSettings,
) -> None:
    """Advance beyond stored history while deriving TTLs from server now."""
    observation = await exercise_generation_allocator(clickhouse_client)
    await write_generation_evidence(clickhouse_settings, observation)

    assert observation.latest_generation_at == observation.seed_server_now + SIMULATED_ROLLBACK
    assert observation.written_at < observation.latest_generation_at
    assert observation.generation_at == observation.latest_generation_at + MICROSECOND
    assert observation.visible_until == observation.written_at + RESULT_TTL
    assert observation.purge_at == observation.written_at + PURGE_TTL
    assert observation.visible_until < observation.purge_at
