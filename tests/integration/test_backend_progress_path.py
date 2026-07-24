"""Exercise the public progress contract against real ClickHouse."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, Final
from uuid import uuid4

import pytest
from taskiq.depends.progress_tracker import TaskProgress, TaskState
from taskiq.result import TaskiqResult
from taskiq.serializers.pickle import PickleSerializer

from taskiq_clickhouse.backend import ClickHouseResultBackend


if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from taskiq.abc.serializer import TaskiqSerializer

    from tests.integration.settings import ClickHouseTestSettings


pytestmark = [pytest.mark.clickhouse, pytest.mark.asyncio]

_RESULT_TTL: Final = timedelta(hours=1)
_PURGE_TTL: Final = timedelta(days=1)
_EXPIRING_TTL: Final = timedelta(seconds=2)
_EXPIRING_PURGE_TTL: Final = timedelta(seconds=8)
_SCENARIO_TIMEOUT_SECONDS: Final = 18.0
_JSON_META: Final = {
    "completed": 2,
    "nested": [True, None, {"step": "decode"}],
}
_PICKLE_META: Final = {
    "created_at": datetime(2026, 7, 16, tzinfo=UTC),
    "members": {"alpha", "beta"},
    "coordinates": (1, 2),
}


@dataclass(frozen=True, slots=True)
class _BackendOptions:
    """Public backend policies varied by one integration scenario."""

    keep_results: bool
    serializer: TaskiqSerializer | None = None
    result_ttl: timedelta = _RESULT_TTL
    purge_ttl: timedelta = _PURGE_TTL


def _backend(
    settings: ClickHouseTestSettings,
    database: str,
    options: _BackendOptions,
) -> ClickHouseResultBackend[Any]:
    host = "localhost" if ":" in settings.host else settings.host
    return ClickHouseResultBackend(
        host=host,
        port=settings.port,
        username=settings.username,
        password=settings.password,
        database=database,
        secure=False,
        result_ttl=options.result_ttl,
        purge_ttl=options.purge_ttl,
        namespace=f"public-progress-{uuid4().hex}",
        keep_results=options.keep_results,
        serializer=options.serializer,
    )


@asynccontextmanager
async def _running_backend(
    settings: ClickHouseTestSettings,
    database: str,
    options: _BackendOptions,
) -> AsyncIterator[ClickHouseResultBackend[Any]]:
    backend = _backend(settings, database, options)
    try:
        await backend.startup()
        yield backend
    finally:
        await backend.shutdown()


def _result(return_value: object) -> TaskiqResult[Any]:
    return TaskiqResult(
        is_err=False,
        log="progress-isolation-log",
        return_value=return_value,
        execution_time=0.125,
        labels={"source": "progress-integration"},
        error=None,
    )


@pytest.mark.parametrize(
    ("serializer", "meta"),
    [
        pytest.param(None, _JSON_META, id="json"),
        pytest.param(PickleSerializer(), _PICKLE_META, id="pickle"),
    ],
)
async def test_public_progress_round_trip_is_non_consuming_and_result_independent(
    serializer: TaskiqSerializer | None,
    meta: object,
    clickhouse_settings: ClickHouseTestSettings,
    clickhouse_database: str,
) -> None:
    """Prove latest progress and result streams remain independent at one task id."""
    async with asyncio.timeout(_SCENARIO_TIMEOUT_SECONDS):
        async with _running_backend(
            clickhouse_settings,
            clickhouse_database,
            _BackendOptions(keep_results=False, serializer=serializer),
        ) as backend:
            task_id = "shared-result-progress-id"
            older = TaskProgress(state=TaskState.STARTED, meta={"superseded": True})
            latest = TaskProgress(state="CUSTOM", meta=meta)

            assert await backend.get_progress("missing-progress") is None
            await backend.set_progress(task_id, older)
            await backend.set_progress(task_id, latest)

            assert not await backend.is_result_ready(task_id)
            assert await backend.get_progress(task_id) == latest
            assert await backend.get_progress(task_id) == latest

            await backend.set_result("result-only", _result("result-only"))
            assert await backend.get_progress("result-only") is None

            await backend.set_result(task_id, _result({"answer": 42}))
            assert await backend.is_result_ready(task_id)
            assert await backend.get_progress(task_id) == latest

            selected = await backend.get_result(task_id, with_logs=True)

            assert selected.return_value == {"answer": 42}
            assert selected.log == "progress-isolation-log"
            assert not await backend.is_result_ready(task_id)
            assert await backend.get_progress(task_id) == latest
            assert await backend.get_progress(task_id) == latest


async def test_public_progress_latest_update_expires_without_consumption(
    clickhouse_settings: ClickHouseTestSettings,
    clickhouse_database: str,
) -> None:
    """Return the latest update repeatedly, then expose logical expiration as missing."""
    async with asyncio.timeout(_SCENARIO_TIMEOUT_SECONDS):
        async with _running_backend(
            clickhouse_settings,
            clickhouse_database,
            _BackendOptions(
                keep_results=False,
                result_ttl=_EXPIRING_TTL,
                purge_ttl=_EXPIRING_PURGE_TTL,
            ),
        ) as backend:
            task_id = "expiring-progress"
            older = TaskProgress(state=TaskState.STARTED, meta={"sequence": 1})
            latest = TaskProgress(state="CUSTOM", meta={"sequence": 2})
            await backend.set_progress(task_id, older)
            await backend.set_progress(task_id, latest)

            assert await backend.get_progress(task_id) == latest
            assert await backend.get_progress(task_id) == latest

            await _wait_for_progress_retention_deadline()

            assert await backend.get_progress(task_id) is None
            assert not await backend.is_result_ready(task_id)


async def _wait_for_progress_retention_deadline() -> None:
    """Wait one full TTL after acknowledged writes before the next server read."""
    deadline_reached = asyncio.Event()
    callback = asyncio.get_running_loop().call_later(
        _EXPIRING_TTL.total_seconds(),
        deadline_reached.set,
    )
    try:
        await deadline_reached.wait()
    finally:
        callback.cancel()
