"""Local fixtures for Taskiq public-boundary tests."""

from collections.abc import AsyncIterator
from typing import Any

import pytest_asyncio
from taskiq import InMemoryBroker

from taskiq_clickhouse import ResultPersistenceReceiver
from taskiq_clickhouse.backend import ClickHouseResultBackend
from tests.factories.receiver import ResultPersistenceReceiverFactory
from tests.taskiq_boundary.models import ReceiverFailureCase
from tests.taskiq_boundary.taskiq_actions import (
    build_receiver_failure_case,
    make_unstarted_backend,
)


@pytest_asyncio.fixture
async def broker() -> AsyncIterator[InMemoryBroker]:
    """Yield a real Taskiq broker and release its thread pool."""
    instance = InMemoryBroker()
    try:
        yield instance
    finally:
        await instance.shutdown()


@pytest_asyncio.fixture
async def result_persistence_receiver(
    broker: InMemoryBroker,
) -> ResultPersistenceReceiver:
    """Build the default receiver after its function-scoped broker."""
    return ResultPersistenceReceiverFactory.build(broker=broker)


@pytest_asyncio.fixture
async def unstarted_clickhouse_backend() -> AsyncIterator[ClickHouseResultBackend[Any]]:
    """Yield a NEW backend and always close its lifecycle shell."""
    backend = make_unstarted_backend()
    try:
        yield backend
    finally:
        await backend.shutdown()


@pytest_asyncio.fixture
async def receiver_failure_case(
    unstarted_clickhouse_backend: ClickHouseResultBackend[Any],
) -> AsyncIterator[ReceiverFailureCase]:
    """Yield one real receiver wired to the unstarted backend."""
    scenario = build_receiver_failure_case(unstarted_clickhouse_backend)
    try:
        yield scenario
    finally:
        await scenario.broker.shutdown()
