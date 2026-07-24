"""Verify bounded cross-loop executor admission race policies."""

from __future__ import annotations

import asyncio
from functools import partial
from typing import TYPE_CHECKING, cast

import pytest

import taskiq_clickhouse._executor_admission as admission_module
from taskiq_clickhouse._executor_admission import SubmissionAdmission


if TYPE_CHECKING:
    from collections.abc import Callable


class _DeferredLoop:
    """Create real futures while retaining cross-loop delivery callbacks."""

    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self.loop = loop
        self.deliveries: list[Callable[[], object]] = []

    def create_future(self) -> asyncio.Future[None]:
        return self.loop.create_future()

    def call_soon_threadsafe(
        self,
        callback: Callable[..., object],
        *args: object,
    ) -> None:
        self.deliveries.append(partial(callback, *args))

    def deliver(self) -> None:
        deliveries, self.deliveries = self.deliveries, []
        for delivery in deliveries:
            delivery()


class _ClosedLoop(_DeferredLoop):
    """Reject a grant exactly as an event loop closed before handoff."""

    def call_soon_threadsafe(
        self,
        callback: Callable[..., object],
        *args: object,
    ) -> None:
        del callback, args
        message = "event loop is closed"
        raise RuntimeError(message)


class _CancelDuringScheduleLoop(_DeferredLoop):
    """Complete cancellation while a cross-loop grant is being scheduled."""

    waiting_task: asyncio.Task[object] | None = None

    def call_soon_threadsafe(
        self,
        callback: Callable[..., object],
        *args: object,
    ) -> None:
        waiting_task = self.waiting_task
        assert waiting_task is not None
        waiting_task.cancel("handoff-cancellation")
        callback(*args)
        message = "loop closed after concurrent cancellation"
        raise RuntimeError(message)


def _install_loop(
    monkeypatch: pytest.MonkeyPatch,
    loop: _DeferredLoop,
) -> None:
    monkeypatch.setattr(
        admission_module,
        "get_running_loop",
        lambda: cast("asyncio.AbstractEventLoop", loop),
    )


async def _queue_waiter(admission: SubmissionAdmission) -> asyncio.Task[object]:
    waiting = asyncio.create_task(admission.acquire())
    await asyncio.sleep(0)
    assert not waiting.done()
    return cast("asyncio.Task[object]", waiting)


async def _assert_capacity_recovered(admission: SubmissionAdmission) -> None:
    permit = await admission.acquire()
    permit.release()


@pytest.mark.asyncio
async def test_permit_release_is_idempotent_without_capacity_growth() -> None:
    """Never turn a duplicate owner release into an additional slot."""
    admission = SubmissionAdmission()
    permit = await admission.acquire()

    permit.release()
    permit.release()
    first = await admission.acquire()
    waiting = asyncio.create_task(admission.acquire())
    await asyncio.sleep(0)

    assert not waiting.done()
    first.release()
    second = await waiting
    second.release()


@pytest.mark.asyncio
async def test_capacity_allows_only_the_configured_active_permits() -> None:
    """Honor capacities greater than one without admitting an extra owner."""
    admission = SubmissionAdmission(capacity=2)
    first = await admission.acquire()
    second = await admission.acquire()
    waiting = asyncio.create_task(admission.acquire())
    await asyncio.sleep(0)

    assert not waiting.done()
    first.release()
    third = await waiting

    second.release()
    third.release()
    recovered = (await admission.acquire(), await admission.acquire())
    for permit in recovered:
        permit.release()


@pytest.mark.asyncio
async def test_waiters_receive_capacity_in_ticket_order() -> None:
    """Transfer a serial slot to queued callers in exact FIFO order."""
    admission = SubmissionAdmission()
    owner = await admission.acquire()
    first_waiter = asyncio.create_task(admission.acquire())
    await asyncio.sleep(0)
    second_waiter = asyncio.create_task(admission.acquire())
    await asyncio.sleep(0)

    owner.release()
    first = await first_waiter
    await asyncio.sleep(0)
    assert not second_waiter.done()

    first.release()
    second = await second_waiter
    second.release()


@pytest.mark.asyncio
async def test_cancelled_grant_releases_slot_before_deferred_delivery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reclaim a granted slot if caller cancellation wins before delivery."""
    admission = SubmissionAdmission()
    first = await admission.acquire()
    deferred_loop = _DeferredLoop(asyncio.get_running_loop())
    _install_loop(monkeypatch, deferred_loop)
    waiting = await _queue_waiter(admission)

    first.release()
    waiting.cancel("granted-cancellation")
    await asyncio.sleep(0)
    deferred_loop.deliver()

    with pytest.raises(asyncio.CancelledError) as raised:
        await waiting
    assert raised.value.args == ("granted-cancellation",)
    await _assert_capacity_recovered(admission)


@pytest.mark.asyncio
async def test_cancelled_future_is_reclaimed_by_pending_delivery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reclaim a grant when its future is cancelled before callback delivery."""
    admission = SubmissionAdmission()
    first = await admission.acquire()
    deferred_loop = _DeferredLoop(asyncio.get_running_loop())
    _install_loop(monkeypatch, deferred_loop)
    waiting = await _queue_waiter(admission)

    first.release()
    waiting.cancel("future-cancellation")
    deferred_loop.deliver()

    with pytest.raises(asyncio.CancelledError) as raised:
        await waiting
    assert raised.value.args == ("future-cancellation",)
    await _assert_capacity_recovered(admission)


@pytest.mark.asyncio
async def test_closed_waiter_loop_is_skipped_without_losing_capacity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Transfer a slot back when the selected waiter's loop is already closed."""
    admission = SubmissionAdmission()
    first = await admission.acquire()
    closed_loop = _ClosedLoop(asyncio.get_running_loop())
    _install_loop(monkeypatch, closed_loop)
    waiting = await _queue_waiter(admission)

    first.release()
    waiting.cancel("closed-loop-cancellation")

    with pytest.raises(asyncio.CancelledError):
        await waiting
    await _assert_capacity_recovered(admission)


@pytest.mark.asyncio
async def test_concurrent_cancellation_owns_slot_when_scheduling_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Avoid a double release if cancellation already reclaimed a failed handoff."""
    admission = SubmissionAdmission()
    first = await admission.acquire()
    cancelling_loop = _CancelDuringScheduleLoop(asyncio.get_running_loop())
    _install_loop(monkeypatch, cancelling_loop)
    waiting = await _queue_waiter(admission)
    cancelling_loop.waiting_task = waiting

    first.release()

    with pytest.raises(asyncio.CancelledError) as raised:
        await waiting
    assert raised.value.args == ("handoff-cancellation",)
    await _assert_capacity_recovered(admission)
