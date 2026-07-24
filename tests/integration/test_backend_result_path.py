"""Exercise the public Taskiq result backend against real ClickHouse."""

from datetime import timedelta
from typing import Any

import pytest
from taskiq.result import TaskiqResult

from taskiq_clickhouse.backend import ClickHouseResultBackend
from taskiq_clickhouse.exceptions import ClickHouseResultNotFoundError
from tests.integration.settings import ClickHouseTestSettings


pytestmark = [pytest.mark.clickhouse, pytest.mark.asyncio]

_RESULT_TTL = timedelta(hours=1)
_PURGE_TTL = timedelta(days=1)


def _backend(
    settings: ClickHouseTestSettings,
    database: str,
    *,
    keep_results: bool,
) -> ClickHouseResultBackend[Any]:
    host = "localhost" if ":" in settings.host else settings.host
    return ClickHouseResultBackend(
        host=host,
        port=settings.port,
        username=settings.username,
        password=settings.password,
        database=database,
        secure=False,
        result_ttl=_RESULT_TTL,
        purge_ttl=_PURGE_TTL,
        namespace="public-backend",
        keep_results=keep_results,
    )


def _result(log: str) -> TaskiqResult[Any]:
    return TaskiqResult(
        is_err=False,
        log=log,
        return_value={"nested": [1, True, None, "value"]},
        execution_time=0.125,
        labels={"source": "integration"},
        error=None,
    )


async def test_public_result_round_trip_and_targeted_consume(
    clickhouse_settings: ClickHouseTestSettings,
    clickhouse_database: str,
) -> None:
    """Cross startup, split-log reads and acknowledged best-effort consume."""
    retained = _backend(clickhouse_settings, clickhouse_database, keep_results=True)
    try:
        await retained.startup()
        await retained.set_result("retained", _result("retained-log"))

        assert await retained.is_result_ready("retained")
        without_log = await retained.get_result("retained", with_logs=False)
        with_log = await retained.get_result("retained", with_logs=True)

        assert without_log.log is None
        assert with_log.log == "retained-log"
        assert without_log.return_value == with_log.return_value
        assert with_log.labels == {"source": "integration"}
    finally:
        await retained.shutdown()

    consumed = _backend(clickhouse_settings, clickhouse_database, keep_results=False)
    try:
        await consumed.startup()
        await consumed.set_result("consumed", _result("consume-log"))

        selected = await consumed.get_result("consumed", with_logs=True)

        assert selected.log == "consume-log"
        assert not await consumed.is_result_ready("consumed")
        with pytest.raises(ClickHouseResultNotFoundError):
            await consumed.get_result("consumed", with_logs=False)
    finally:
        await consumed.shutdown()
