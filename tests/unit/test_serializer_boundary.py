"""Verify terminal ownership edge cases of synchronous boundary futures."""

import asyncio
from collections.abc import Callable
from concurrent.futures import Executor, ThreadPoolExecutor
from contextvars import ContextVar
from functools import partial
from threading import Event, get_ident

import pytest
from taskiq.abc.serializer import TaskiqSerializer

from taskiq_clickhouse._executor_admission import SubmissionAdmission, SubmissionPermit
from taskiq_clickhouse._executor_pool import ProcessThreadPool
from taskiq_clickhouse._serializer_boundary import (
    BoundaryFailure,
    CapturedValue,
    SerializerBoundary,
    _drain_boundary_future,
    run_boundary,
)
from taskiq_clickhouse.exceptions import ClickHouseDecodeError, ClickHouseEncodeError


_THREAD_TIMEOUT = 2.0
_PAIR_SIZE = 2
_CALLER_CONTEXT: ContextVar[str] = ContextVar("serializer-boundary-context", default="default")


class _FatalBoundarySignal(BaseException):
    """Synthetic terminal signal whose exact identity must survive."""


class _AcquireFailurePool(ProcessThreadPool):
    """Fail before one executor submission permit can be acquired."""

    async def acquire(self) -> SubmissionPermit:
        """Expose one ordinary process-pool admission failure."""
        message = "executor admission unavailable"
        raise RuntimeError(message)


class _CreationFailurePool(ProcessThreadPool):
    """Fail while the admitted caller requests a process-owned executor."""

    @property
    def executor(self) -> ThreadPoolExecutor:
        """Expose one ordinary lazy pool-creation failure."""
        message = "executor creation unavailable"
        raise RuntimeError(message)


class _BlockingSerializer(TaskiqSerializer):
    """Expose whether two shared-boundary calls overlap in worker threads."""

    def __init__(self) -> None:
        self.first_started = Event()
        self.first_release = Event()
        self.second_started = Event()
        self.calls = 0

    def dumpb(self, value: object) -> bytes:
        del value
        self.calls += 1
        if self.calls == 1:
            self.first_started.set()
            if not self.first_release.wait(timeout=_THREAD_TIMEOUT):
                message = "serializer release timeout"
                raise RuntimeError(message)
        else:
            self.second_started.set()
        return b"serialized"

    def loadb(self, value: bytes) -> object:
        return value


class _HostileMethodLookupSerializer(TaskiqSerializer):
    """Raise secret-bearing text whenever a serializer method is resolved."""

    def __init__(self) -> None:
        self.lookup_threads: list[int] = []

    def __getattribute__(self, attribute_name: str) -> object:
        if attribute_name in {"dumpb", "loadb"}:
            lookup_threads = object.__getattribute__(self, "lookup_threads")
            lookup_threads.append(get_ident())
            message = "password=serializer-method-secret"  # pragma: allowlist secret
            raise RuntimeError(message)
        return super().__getattribute__(attribute_name)

    def dumpb(self, value: object) -> bytes:
        del value
        return b"unreachable"

    def loadb(self, value: bytes) -> object:
        return value


def _boundary_future() -> asyncio.Future[CapturedValue[object] | BoundaryFailure]:
    return asyncio.get_running_loop().create_future()


async def _wait_thread_event(event: Event) -> None:
    observed = await asyncio.to_thread(event.wait, _THREAD_TIMEOUT)
    assert observed


def _blocking_operation(started: Event, release: Event, value: str) -> str:
    started.set()
    if not release.wait(timeout=_THREAD_TIMEOUT):
        message = "boundary release timeout"
        raise RuntimeError(message)
    return value


def _observable_operation(started: Event, value: str) -> str:
    started.set()
    return value


def _read_and_replace_context() -> str:
    observed = _CALLER_CONTEXT.get()
    _CALLER_CONTEXT.set("worker-mutated-context")
    return observed


def _captured_value(
    outcome: CapturedValue[str] | BoundaryFailure,
) -> str:
    assert isinstance(outcome, CapturedValue)
    return outcome.boundary_value


@pytest.mark.asyncio
async def test_drain_accepts_a_cancelled_terminal_future() -> None:
    """Keep dependency future cancellation subordinate to outer cancellation."""
    boundary_future = _boundary_future()
    boundary_future.cancel("dependency-cancellation")

    await _drain_boundary_future(boundary_future)

    assert boundary_future.cancelled()


@pytest.mark.asyncio
async def test_drain_preserves_an_already_terminal_fatal_signal() -> None:
    """Surface a fatal outcome even when it wins the race before drain starts."""
    boundary_future = _boundary_future()
    fatal = _FatalBoundarySignal()
    boundary_future.set_exception(fatal)

    with pytest.raises(_FatalBoundarySignal) as raised:
        await _drain_boundary_future(boundary_future)

    assert raised.value is fatal


@pytest.mark.asyncio
async def test_drain_subordinates_an_ordinary_future_failure() -> None:
    """Keep executor infrastructure failure subordinate to caller cancellation."""
    boundary_future = _boundary_future()
    asyncio.get_running_loop().call_soon(
        boundary_future.set_exception,
        RuntimeError("executor future unavailable"),
    )

    await _drain_boundary_future(boundary_future)

    assert isinstance(boundary_future.exception(), RuntimeError)


@pytest.mark.asyncio
async def test_drain_ignores_additional_cancellation_until_future_terminates() -> None:
    """Keep ownership of submitted work when cancellation repeats during drain."""
    boundary_future = _boundary_future()
    draining = asyncio.create_task(_drain_boundary_future(boundary_future))
    await asyncio.sleep(0)

    draining.cancel("repeated-cancellation")
    await asyncio.sleep(0)
    assert not draining.done()

    boundary_future.set_result(CapturedValue("completed"))
    await draining


@pytest.mark.asyncio
async def test_submission_limit_hands_off_without_executor_overlap() -> None:
    """Submit no more jobs than the finite process-local admission allows."""
    pool = ProcessThreadPool(
        thread_name_prefix="bounded-overlap-test",
        max_workers=2,
        submission_limit=1,
    )
    first_started = Event()
    first_release = Event()
    second_started = Event()
    first = asyncio.create_task(
        run_boundary(
            partial(_blocking_operation, first_started, first_release, "first"),
            executor=pool,
        ),
    )
    second: asyncio.Task[CapturedValue[str] | BoundaryFailure] | None = None
    try:
        await _wait_thread_event(first_started)
        second = asyncio.create_task(
            run_boundary(
                partial(_observable_operation, second_started, "second"),
                executor=pool,
            ),
        )
        await asyncio.sleep(0)
        assert not second.done()
        assert not second_started.is_set()

        first_release.set()
        await _wait_thread_event(second_started)
        assert _captured_value(await first) == "first"
        assert _captured_value(await second) == "second"
    finally:
        first_release.set()
        await asyncio.gather(
            first,
            *(() if second is None else (second,)),
            return_exceptions=True,
        )
        pool.executor.shutdown()


@pytest.mark.asyncio
async def test_waiting_submission_cancels_without_entering_executor() -> None:
    """Remove a cancelled waiter without retaining or executing its operation."""
    pool = ProcessThreadPool(
        thread_name_prefix="bounded-cancellation-test",
        max_workers=2,
        submission_limit=1,
    )
    first_started = Event()
    first_release = Event()
    cancelled_started = Event()
    first = asyncio.create_task(
        run_boundary(
            partial(_blocking_operation, first_started, first_release, "first"),
            executor=pool,
        ),
    )
    waiting: asyncio.Task[CapturedValue[str] | BoundaryFailure] | None = None
    try:
        await _wait_thread_event(first_started)
        waiting = asyncio.create_task(
            run_boundary(
                partial(_observable_operation, cancelled_started, "cancelled"),
                executor=pool,
            ),
        )
        await asyncio.sleep(0)
        waiting.cancel("waiting-cancellation")
        with pytest.raises(asyncio.CancelledError) as raised:
            await waiting

        assert raised.value.args == ("waiting-cancellation",)
        assert not cancelled_started.is_set()
        assert not first.done()
    finally:
        first_release.set()
        await asyncio.gather(
            first,
            *(() if waiting is None else (waiting,)),
            return_exceptions=True,
        )
        pool.executor.shutdown()


@pytest.mark.asyncio
async def test_rejected_executor_submission_returns_its_admission() -> None:
    """Classify rejected submission and release its exact admission slot."""
    pool = ProcessThreadPool(
        thread_name_prefix="rejected-submission-test",
        max_workers=1,
        submission_limit=1,
    )
    pool.executor.shutdown()

    outcome = await run_boundary(lambda: "never-submitted", executor=pool)

    assert outcome is BoundaryFailure.EXECUTOR_UNAVAILABLE
    recovered = await asyncio.wait_for(pool.acquire(), timeout=_THREAD_TIMEOUT)
    recovered.release()


@pytest.mark.asyncio
async def test_executor_acquire_failure_is_distinct_from_boundary_failure() -> None:
    """Classify process-pool admission failure without blaming hook behavior."""
    pool = _AcquireFailurePool(thread_name_prefix="acquire-failure-test")

    outcome = await run_boundary(lambda: "never-submitted", executor=pool)

    assert outcome is BoundaryFailure.EXECUTOR_UNAVAILABLE


@pytest.mark.asyncio
async def test_executor_creation_failure_returns_its_admission() -> None:
    """Classify lazy pool creation failure and recover the acquired slot."""
    pool = _CreationFailurePool(
        thread_name_prefix="creation-failure-test",
        submission_limit=1,
    )

    outcome = await run_boundary(lambda: "never-submitted", executor=pool)

    assert outcome is BoundaryFailure.EXECUTOR_UNAVAILABLE
    recovered = await asyncio.wait_for(pool.acquire(), timeout=_THREAD_TIMEOUT)
    recovered.release()


@pytest.mark.asyncio
async def test_executor_future_failure_returns_its_admission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Classify an ordinary submitted-future failure without retaining it."""
    pool = ProcessThreadPool(
        thread_name_prefix="future-failure-test",
        max_workers=1,
        submission_limit=1,
    )
    loop = asyncio.get_running_loop()

    def failed_future(
        executor: Executor | None,
        callback: Callable[..., object],
        *args: object,
    ) -> asyncio.Future[object]:
        del executor, callback, args
        future: asyncio.Future[object] = loop.create_future()
        future.set_exception(RuntimeError("executor future unavailable"))
        return future

    monkeypatch.setattr(loop, "run_in_executor", failed_future)
    try:
        outcome = await run_boundary(lambda: "never-completed", executor=pool)
        recovered = await asyncio.wait_for(pool.acquire(), timeout=_THREAD_TIMEOUT)
    finally:
        pool.executor.shutdown()

    assert outcome is BoundaryFailure.EXECUTOR_UNAVAILABLE
    recovered.release()


@pytest.mark.asyncio
async def test_shared_serializer_admission_prevents_concurrent_calls() -> None:
    """Serialize dump/load access when result and progress share one instance."""
    serializer = _BlockingSerializer()
    admission = SubmissionAdmission()
    first_boundary = SerializerBoundary(serializer, admission)
    second_boundary = SerializerBoundary(serializer, admission)
    first = asyncio.create_task(
        first_boundary.dump(
            object(),
            operation="test",
            failed_reason="failed",
            type_reason="type",
        ),
    )
    second: asyncio.Task[bytes] | None = None
    try:
        await _wait_thread_event(serializer.first_started)
        second = asyncio.create_task(
            second_boundary.dump(
                object(),
                operation="test",
                failed_reason="failed",
                type_reason="type",
            ),
        )
        await asyncio.sleep(0)
        assert not second.done()
        assert not serializer.second_started.is_set()
    finally:
        serializer.first_release.set()
    assert await first == b"serialized"
    assert second is not None
    assert await second == b"serialized"
    assert serializer.calls == _PAIR_SIZE


@pytest.mark.asyncio
async def test_dump_method_lookup_runs_inside_the_safe_worker_boundary() -> None:
    """Resolve an untrusted dump descriptor off-loop and redact its failure."""
    serializer = _HostileMethodLookupSerializer()
    loop_thread = get_ident()

    with pytest.raises(ClickHouseEncodeError, match="failed") as raised:
        await SerializerBoundary(serializer).dump(
            object(),
            operation="test",
            failed_reason="failed",
            type_reason="type",
        )

    assert serializer.lookup_threads
    assert all(thread_id != loop_thread for thread_id in serializer.lookup_threads)
    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None
    assert "serializer-method-secret" not in repr(raised.value) + str(raised.value)


@pytest.mark.asyncio
async def test_load_method_lookup_runs_inside_the_safe_worker_boundary() -> None:
    """Resolve an untrusted load descriptor off-loop and redact its failure."""
    serializer = _HostileMethodLookupSerializer()
    loop_thread = get_ident()

    with pytest.raises(ClickHouseDecodeError, match="failed") as raised:
        await SerializerBoundary(serializer).load(
            b"payload",
            operation="test",
            failed_reason="failed",
        )

    assert serializer.lookup_threads
    assert all(thread_id != loop_thread for thread_id in serializer.lookup_threads)
    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None
    assert "serializer-method-secret" not in repr(raised.value) + str(raised.value)


@pytest.mark.asyncio
async def test_each_submission_receives_an_isolated_caller_context() -> None:
    """Propagate task-local context without leaking values across worker jobs."""
    pool = ProcessThreadPool(
        thread_name_prefix="context-boundary-test",
        max_workers=1,
        submission_limit=1,
    )

    async def read_in_context(value: str) -> str:
        token = _CALLER_CONTEXT.set(value)
        try:
            return _captured_value(
                await run_boundary(_read_and_replace_context, executor=pool),
            )
        finally:
            _CALLER_CONTEXT.reset(token)

    try:
        observed = await asyncio.gather(
            read_in_context("first-context"),
            read_in_context("second-context"),
        )
        default_outcome = await run_boundary(_CALLER_CONTEXT.get, executor=pool)
    finally:
        pool.executor.shutdown()

    assert tuple(observed) == ("first-context", "second-context")
    assert _captured_value(default_outcome) == "default"
