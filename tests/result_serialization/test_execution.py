"""Verify result model and serializer executor ownership."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, Event, Lock, get_ident
from typing import Any

import pytest
from taskiq.result import TaskiqResult

import taskiq_clickhouse._result_model as result_model_module
from taskiq_clickhouse._serialization import EncodedResult, ResultCodec
import taskiq_clickhouse._serializer_boundary as serializer_boundary_module
from taskiq_clickhouse._serializer_boundary import BoundaryFailure, CapturedValue
from tests.factories.results import TaskiqResultFactory
from tests.result_serialization.execution_doubles import (
    BlockingSerializer,
    ConcurrentSerializer,
    FatalBoundarySignal,
    ThreadProbeSerializer,
)


_PAIR_SIZE = 2
_ASSERTION_TIMEOUT = 2.0


async def _wait_async_event(event: asyncio.Event) -> None:
    """Fail if one deterministic async synchronization signal never arrives."""
    async with asyncio.timeout(_ASSERTION_TIMEOUT):
        await event.wait()


async def _wait_thread_event(event: Event) -> None:
    """Fail if one deterministic worker-thread signal never arrives."""
    observed = await asyncio.to_thread(event.wait, _ASSERTION_TIMEOUT)
    assert observed


@pytest.mark.asyncio
async def test_dependency_fatal_signal_keeps_identity() -> None:
    """Translate ordinary failures without swallowing fatal signals."""
    fatal = FatalBoundarySignal()
    release = Event()
    release.set()
    serializer = BlockingSerializer(Event(), release, terminal_error=fatal)
    codec = ResultCodec(serializer)

    with pytest.raises(FatalBoundarySignal) as raised:
        await codec.encode(TaskiqResultFactory.build())

    assert raised.value is fatal


@pytest.mark.asyncio
async def test_model_and_serializer_work_use_executor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep all synchronous model and serializer work off the event loop."""
    event_loop_thread = get_ident()
    model_threads: list[int] = []
    source = TaskiqResultFactory.build(return_value={"thread": "placement"})
    original_dump = source.model_dump
    result_model = result_model_module._RESULT_MODEL  # noqa: SLF001 - focused boundary probe.
    original_validate = result_model.model_validate

    def observed_dump(*args: object, **kwargs: object) -> dict[str, Any]:
        del args, kwargs
        model_threads.append(get_ident())
        return original_dump(mode="python")

    class _ObservedResultModel:
        @classmethod
        def model_validate(cls, candidate: object, *, strict: bool) -> TaskiqResult[Any]:
            del cls
            model_threads.append(get_ident())
            return original_validate(candidate, strict=strict)

    monkeypatch.setattr(source.__class__, "model_dump", observed_dump)
    monkeypatch.setattr(result_model_module, "_RESULT_MODEL", _ObservedResultModel)
    serializer = ThreadProbeSerializer()
    codec = ResultCodec(serializer)

    encoded = await codec.encode(source)
    await codec.decode(encoded.result_payload, encoded.log_payload)

    assert len(model_threads) == _PAIR_SIZE
    assert all(thread_id != event_loop_thread for thread_id in model_threads)
    assert len(serializer.dump_thread_ids) == _PAIR_SIZE
    assert len(serializer.load_thread_ids) == _PAIR_SIZE
    assert all(
        thread_id != event_loop_thread for thread_id in (*serializer.dump_thread_ids, *serializer.load_thread_ids)
    )


def test_dedicated_lane_serializes_model_dumps_but_not_serializer_executors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Protect Taskiq's dump cache without blocking serializer workers."""
    state_lock = Lock()
    start_barrier = Barrier(_PAIR_SIZE)
    model_entered = Event()
    model_release = Event()
    active_models = 0
    max_active_models = 0
    source_a = TaskiqResultFactory.build(return_value="a")
    source_b = TaskiqResultFactory.build(return_value="b")
    result_class = source_a.__class__
    original_dump = result_class.model_dump
    serializer = ConcurrentSerializer()

    def observed_dump(
        instance: TaskiqResult[Any],
        *args: object,
        **kwargs: object,
    ) -> dict[str, Any]:
        del args, kwargs
        nonlocal active_models, max_active_models
        with state_lock:
            active_models += 1
            max_active_models = max(max_active_models, active_models)
        model_entered.set()
        try:
            model_release.wait()
            return original_dump(instance, mode="python")
        finally:
            with state_lock:
                active_models -= 1

    def run_encode(source: TaskiqResult[Any]) -> EncodedResult:
        start_barrier.wait(timeout=2)
        return asyncio.run(ResultCodec(serializer).encode(source))

    monkeypatch.setattr(result_class, "model_dump", observed_dump)
    with ThreadPoolExecutor(max_workers=_PAIR_SIZE) as executor:
        futures = tuple(executor.submit(run_encode, source) for source in (source_a, source_b))
        try:
            assert model_entered.wait(timeout=_ASSERTION_TIMEOUT)
            assert all(not future.done() for future in futures)
        finally:
            model_release.set()
        encoded = tuple(future.result(timeout=_ASSERTION_TIMEOUT) for future in futures)

    assert len(encoded) == _PAIR_SIZE
    assert max_active_models == 1
    assert serializer.max_active == _PAIR_SIZE


@pytest.mark.asyncio
async def test_encode_cancellation_propagates_without_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Drain the exact executor job before restoring the first cancellation."""
    started = Event()
    release = Event()
    drain_entered = asyncio.Event()
    drain_release = asyncio.Event()
    repeated_cancellation_observed = asyncio.Event()
    original_drain = serializer_boundary_module._drain_boundary_future  # noqa: SLF001 - cancellation seam.

    async def observed_drain(
        boundary_future: asyncio.Future[CapturedValue[object] | BoundaryFailure],
    ) -> None:
        drain_entered.set()
        try:
            await drain_release.wait()
        except asyncio.CancelledError:
            repeated_cancellation_observed.set()
        await original_drain(boundary_future)

    monkeypatch.setattr(
        serializer_boundary_module,
        "_drain_boundary_future",
        observed_drain,
    )
    serializer = BlockingSerializer(started, release)
    task = asyncio.create_task(ResultCodec(serializer).encode(TaskiqResultFactory.build()))
    try:
        await _wait_thread_event(started)
        assert task.cancel("first-cancellation")
        await _wait_async_event(drain_entered)
        assert task.cancel("second-cancellation")
        await _wait_async_event(repeated_cancellation_observed)
        assert not task.done()
    finally:
        drain_release.set()
        release.set()
    with pytest.raises(asyncio.CancelledError) as raised:
        await task

    assert serializer.calls == 1
    assert raised.value.args == ("first-cancellation",)


@pytest.mark.asyncio
async def test_terminal_fatal_signal_wins_over_outer_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Surface the exact fatal job outcome only after its job terminates."""
    started = Event()
    release = Event()
    drain_entered = asyncio.Event()
    original_drain = serializer_boundary_module._drain_boundary_future  # noqa: SLF001 - cancellation seam.

    async def observed_drain(
        boundary_future: asyncio.Future[CapturedValue[object] | BoundaryFailure],
    ) -> None:
        drain_entered.set()
        await original_drain(boundary_future)

    monkeypatch.setattr(
        serializer_boundary_module,
        "_drain_boundary_future",
        observed_drain,
    )
    fatal = FatalBoundarySignal()
    serializer = BlockingSerializer(started, release, terminal_error=fatal)
    task = asyncio.create_task(ResultCodec(serializer).encode(TaskiqResultFactory.build()))
    try:
        await _wait_thread_event(started)
        assert task.cancel("outer-cancellation")
        await _wait_async_event(drain_entered)
        assert not task.done()
    finally:
        release.set()
    with pytest.raises(FatalBoundarySignal) as raised:
        await task

    assert raised.value is fatal
    assert serializer.calls == 1


@pytest.mark.asyncio
async def test_blocked_serializer_does_not_starve_asyncio_default_executor() -> None:
    """Isolate an uncooperative serializer from unrelated to-thread work."""
    loop = asyncio.get_running_loop()
    loop.set_default_executor(ThreadPoolExecutor(max_workers=1))
    started = Event()
    release = Event()
    serializer = BlockingSerializer(started, release)
    task = asyncio.create_task(ResultCodec(serializer).encode(TaskiqResultFactory.build()))
    try:
        async with asyncio.timeout(2):
            started_in_time = await asyncio.to_thread(started.wait, 1)
            assert started_in_time
            observed = await asyncio.to_thread(lambda: "default-executor-ready")
    finally:
        release.set()
        await task

    assert observed == "default-executor-ready"
