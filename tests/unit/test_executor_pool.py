"""Verify lazy per-process ownership of synchronous boundary executors."""

import asyncio
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from threading import Event

import pytest

import taskiq_clickhouse._executor_pool as executor_pool_module
from taskiq_clickhouse._executor_pool import ProcessThreadPool


_WAIT_TIMEOUT = 2.0


async def _wait_thread_event(event: Event) -> None:
    observed = await asyncio.to_thread(event.wait, _WAIT_TIMEOUT)
    assert observed


def test_process_thread_pool_is_lazy_stable_and_pid_aware() -> None:
    """Reuse within one PID and never submit through an inherited executor."""
    current_pid = 100

    def pid_factory() -> int:
        return current_pid

    pool = ProcessThreadPool(
        thread_name_prefix="test-boundary",
        pid_factory=pid_factory,
    )
    first = pool.executor
    try:
        assert pool.executor is first
        assert first.submit(lambda: "parent").result() == "parent"

        current_pid = 101
        second = pool.executor
        try:
            assert second is not first
            assert second.submit(lambda: "child").result() == "child"
        finally:
            second.shutdown()
    finally:
        first.shutdown()


def test_process_thread_pool_honors_an_explicit_serial_lane() -> None:
    """Retain the one-worker policy required by Taskiq's model-dump cache."""
    pool = ProcessThreadPool(
        thread_name_prefix="test-serial-boundary",
        max_workers=1,
    )
    executor = pool.executor
    first_started = Event()
    first_release = Event()
    second_started = Event()

    def first_job() -> int:
        first_started.set()
        first_release.wait()
        return 1

    def second_job() -> int:
        second_started.set()
        return 2

    try:
        assert isinstance(executor, ThreadPoolExecutor)
        first = executor.submit(first_job)
        second = executor.submit(second_job)
        assert first_started.wait(timeout=1)
        assert not second_started.is_set()
        first_release.set()
        assert (first.result(), second.result()) == (1, 2)
    finally:
        first_release.set()
        executor.shutdown()


def test_process_thread_pool_rejects_an_empty_submission_capacity() -> None:
    """Reject an internal policy that could never admit executor work."""
    with pytest.raises(ValueError, match="submission capacity must be positive"):
        ProcessThreadPool(
            thread_name_prefix="invalid-admission",
            submission_limit=0,
        )


@pytest.mark.asyncio
async def test_admission_state_rotates_before_executor_creation_on_pid_change() -> None:
    """Never inherit a held parent admission slot into another process."""
    current_pid = 100

    def pid_factory() -> int:
        return current_pid

    pool = ProcessThreadPool(
        thread_name_prefix="pid-admission",
        submission_limit=1,
        pid_factory=pid_factory,
    )
    parent_permit = await pool.acquire()
    current_pid = 101

    child_permit = await pool.acquire()

    child_permit.release()
    parent_permit.release()


@pytest.mark.asyncio
async def test_one_admission_hands_off_between_independent_event_loops() -> None:
    """Wake only the waiter's own loop without an asyncio primitive affinity error."""
    pool = ProcessThreadPool(
        thread_name_prefix="cross-loop-admission",
        submission_limit=1,
    )
    first_permit = await pool.acquire()
    waiter_queued = Event()

    async def acquire_from_other_loop() -> None:
        acquisition = asyncio.create_task(pool.acquire())
        await asyncio.sleep(0)
        waiter_queued.set()
        permit = await acquisition
        permit.release()

    with ThreadPoolExecutor(max_workers=1) as loop_thread:
        other_loop = loop_thread.submit(asyncio.run, acquire_from_other_loop())
        try:
            await _wait_thread_event(waiter_queued)
            assert not other_loop.done()
        finally:
            first_permit.release()
        await asyncio.wrap_future(other_loop)


@pytest.mark.asyncio
async def test_registered_fork_callback_discards_held_parent_admission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reset inherited admission and executor ownership in the child callback."""
    callbacks: list[Callable[[], None]] = []

    def register_at_fork(*, after_in_child: Callable[[], None]) -> None:
        callbacks.append(after_in_child)

    monkeypatch.setattr(
        executor_pool_module,
        "_REGISTER_AT_FORK",
        register_at_fork,
    )
    pool = ProcessThreadPool(
        thread_name_prefix="fork-reset-admission",
        submission_limit=1,
    )
    parent_permit = await pool.acquire()

    assert len(callbacks) == 1
    callbacks[0]()
    child_permit = await pool.acquire()

    child_permit.release()
    parent_permit.release()


def test_pool_remains_portable_without_at_fork_registration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retain lazy executor behavior on platforms without fork callbacks."""
    monkeypatch.setattr(executor_pool_module, "_REGISTER_AT_FORK", None)

    pool = ProcessThreadPool(thread_name_prefix="no-at-fork")
    executor = pool.executor
    try:
        assert executor.submit(lambda: "portable").result() == "portable"
    finally:
        executor.shutdown()
