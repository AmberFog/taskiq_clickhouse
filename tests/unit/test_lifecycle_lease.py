"""Verify lifecycle lease ownership without exposing its raw mutex."""

import asyncio
import os

import pytest

from taskiq_clickhouse._lifecycle_lease import LifecycleLease
from taskiq_clickhouse.exceptions import ClickHouseLifecycleError


_ASSERTION_DEADLINE_SECONDS = 1.0


async def _wait(event: asyncio.Event) -> None:
    async with asyncio.timeout(_ASSERTION_DEADLINE_SECONDS):
        await event.wait()


@pytest.mark.asyncio
async def test_lease_serializes_callers_from_one_event_loop() -> None:
    """Allow one queued participant to enter only after its owner exits."""
    lease = LifecycleLease()
    owner_entered = asyncio.Event()
    owner_release = asyncio.Event()
    contender_called = asyncio.Event()
    observations: list[str] = []

    async def owner() -> None:
        async with lease.hold("owner"):
            observations.append("owner_entered")
            owner_entered.set()
            await owner_release.wait()
            observations.append("owner_leaving")

    async def contender() -> None:
        contender_called.set()
        async with lease.hold("contender"):
            observations.append("contender_entered")

    owner_task = asyncio.create_task(owner())
    await _wait(owner_entered)
    contender_task = asyncio.create_task(contender())
    await _wait(contender_called)

    assert observations == ["owner_entered"]
    owner_release.set()
    await asyncio.gather(owner_task, contender_task)
    assert observations == ["owner_entered", "owner_leaving", "contender_entered"]


@pytest.mark.asyncio
async def test_cancelled_waiter_releases_its_claim() -> None:
    """Remove a cancelled queued participant and leave the lease reusable."""
    lease = LifecycleLease()
    waiter_called = asyncio.Event()

    async with lease.hold("owner"):

        async def wait_for_lease() -> None:
            waiter_called.set()
            async with lease.hold("waiter"):
                pytest.fail("cancelled waiter entered the lease")

        waiter = asyncio.create_task(wait_for_lease())
        await _wait(waiter_called)
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter

    async with lease.hold("successor"):
        pass


@pytest.mark.asyncio
async def test_active_lease_rejects_another_event_loop() -> None:
    """Reject a concurrent loop before it can enter the shared mutex."""
    lease = LifecycleLease()

    async def foreign_call() -> str:
        with pytest.raises(ClickHouseLifecycleError) as raised:
            async with lease.hold("foreign_operation"):
                pytest.fail("foreign loop entered the lease")
        return raised.value.reason

    async with lease.hold("owner"):
        reason = await asyncio.to_thread(lambda: asyncio.run(foreign_call()))

    assert reason == "foreign_runtime"


@pytest.mark.asyncio
async def test_active_lease_rejects_another_process_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reject a changed PID while retaining the original active owner."""
    lease = LifecycleLease()
    owner_pid = os.getpid()

    async with lease.hold("owner"):
        with monkeypatch.context() as scoped_patch:
            scoped_patch.setattr(os, "getpid", lambda: owner_pid + 1)
            with pytest.raises(ClickHouseLifecycleError, match="foreign_runtime"):
                async with lease.hold("foreign_operation"):
                    pytest.fail("foreign process identity entered the lease")


def test_released_lease_can_move_between_event_loops() -> None:
    """Reset loop ownership only after the final participant releases it."""
    lease = LifecycleLease()
    loops: list[asyncio.AbstractEventLoop] = []

    async def use_lease() -> None:
        async with lease.hold("owner"):
            loops.append(asyncio.get_running_loop())

    asyncio.run(use_lease())
    asyncio.run(use_lease())

    assert loops[0] is not loops[1]
