"""Verify the isolated Taskiq progress serialization boundary."""

from __future__ import annotations

import asyncio
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime
from threading import Event, get_ident
from typing import TYPE_CHECKING, Any, cast

import pytest
from taskiq.abc.serializer import TaskiqSerializer
from taskiq.depends.progress_tracker import TaskProgress, TaskState
from taskiq.serializers.json_serializer import JSONSerializer
from taskiq.serializers.pickle import PickleSerializer

import taskiq_clickhouse._progress_serialization as progress_serialization
from taskiq_clickhouse._progress_serialization import ProgressCodec
from taskiq_clickhouse.exceptions import (
    ClickHouseDataCorruptionError,
    ClickHouseDecodeError,
    ClickHouseEncodeError,
)
from tests.serializer_testkit import (
    SERIALIZER_FAILURE_DETAIL as _RAW_ERROR_DETAIL,
    BytesSubclass as _BytesSubclass,
    ExplodingMapping as _ExplodingMapping,
    RecordingSerializer as _RecordingSerializer,
    ScriptedSerializer as _ScriptedSerializer,
    assert_safe_error as _assert_safe_error,
    boundary_unavailable as _boundary_unavailable,
)


if TYPE_CHECKING:
    from collections.abc import Callable
    from concurrent.futures import Executor


_RELEASE_TIMEOUT = "test release timeout"
_PROGRESS_FIELDS = frozenset(("state", "meta"))
_PAIR_SIZE = 2


class _ThreadProbeSerializer(TaskiqSerializer):
    """Record the executor thread used by every serializer call."""

    def __init__(self) -> None:
        self.delegate = PickleSerializer()
        self.thread_ids: list[int] = []

    def dumpb(self, candidate: object) -> bytes:
        self.thread_ids.append(get_ident())
        return self.delegate.dumpb(candidate)

    def loadb(self, payload: bytes) -> object:
        self.thread_ids.append(get_ident())
        return self.delegate.loadb(payload)


class _BlockingSerializer(TaskiqSerializer):
    """Block serializer work until its cancellation test releases it."""

    def __init__(self, started: Event, release: Event) -> None:
        self.started = started
        self.release = release
        self.delegate = PickleSerializer()
        self.calls = 0

    def dumpb(self, candidate: object) -> bytes:
        self.calls += 1
        self.started.set()
        if not self.release.wait(timeout=2):
            raise RuntimeError(_RELEASE_TIMEOUT)
        return self.delegate.dumpb(candidate)

    def loadb(self, payload: bytes) -> object:
        return self.delegate.loadb(payload)


class _ExplodingKey:
    """Fail while dumped model keys are compared with the exact contract."""

    def __hash__(self) -> int:
        return hash("state")

    def __eq__(self, candidate: object) -> bool:
        del candidate
        raise RuntimeError(_RAW_ERROR_DETAIL)


class _FatalProgressSignal(BaseException):
    """Synthetic fatal model signal whose identity must cross the boundary."""


def _progress(
    *,
    state: TaskState | str = TaskState.STARTED,
    meta: object = None,
) -> TaskProgress[Any]:
    return TaskProgress(state=state, meta=meta)


def _progress_mapping(**replacements: object) -> dict[str, object]:
    return {"state": "CUSTOM", "meta": {"completed": 2}, **replacements}


def test_progress_codec_is_frozen_and_hides_serializer_repr() -> None:
    """Keep the configured serializer immutable and out of diagnostics."""
    serializer = _RecordingSerializer()
    codec = ProgressCodec(serializer)

    assert "RecordingSerializer" not in repr(codec)
    with pytest.raises(FrozenInstanceError):
        codec.serializer = PickleSerializer()  # type: ignore[misc]


@pytest.mark.asyncio
async def test_json_round_trip_uses_python_mode_mapping() -> None:
    """Round-trip standard state and JSON-compatible metadata."""
    source = _progress(meta={"completed": 2, "nested": [True, None]})
    codec = ProgressCodec(JSONSerializer(default=None, ensure_ascii=True))

    payload = await codec.encode(source)
    decoded = await codec.decode(payload)

    assert type(decoded.state) is str
    assert decoded.state == TaskState.STARTED.value
    assert decoded.meta == source.meta


@pytest.mark.asyncio
async def test_pickle_round_trip_preserves_python_only_metadata() -> None:
    """Preserve supported Python objects through explicit Pickle opt-in."""
    meta = {
        "created_at": datetime(2026, 7, 16, tzinfo=UTC),
        "members": {"alpha", "beta"},
        "coordinates": (1, 2),
    }
    codec = ProgressCodec(PickleSerializer())

    decoded = await codec.decode(await codec.encode(_progress(state="CUSTOM", meta=meta)))

    assert decoded.state == "CUSTOM"
    assert decoded.meta == meta
    assert isinstance(decoded.meta["members"], set)


@pytest.mark.asyncio
async def test_configured_serializer_receives_exact_mapping() -> None:
    """Use the configured strategy once in each direction without an envelope."""
    serializer = _RecordingSerializer()
    source = _progress(state="CUSTOM", meta={"completed": 2})
    codec = ProgressCodec(serializer)

    payload = await codec.encode(source)
    decoded = await codec.decode(payload)

    assert len(serializer.dumped) == 1
    assert isinstance(serializer.dumped[0], dict)
    assert frozenset(serializer.dumped[0]) == _PROGRESS_FIELDS
    assert serializer.loaded == [payload]
    assert decoded == source


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("event", "reason"),
    [
        pytest.param(RuntimeError(_RAW_ERROR_DETAIL), "progress_payload_encode_failed", id="failure"),
        pytest.param(
            asyncio.CancelledError(_RAW_ERROR_DETAIL),
            "progress_payload_encode_failed",
            id="dependency-cancellation",
        ),
        pytest.param(_BytesSubclass(b"payload"), "progress_payload_not_bytes", id="wrong-type"),
    ],
)
async def test_serializer_dump_failure_is_safe(event: object, reason: str) -> None:
    """Detach custom serializer failures and reject non-exact bytes."""
    codec = ProgressCodec(_ScriptedSerializer(dump_events=[event]))

    with pytest.raises(ClickHouseEncodeError) as raised:
        await codec.encode(_progress())

    _assert_safe_error(raised.value, operation="progress_encode", reason=reason)


@pytest.mark.asyncio
async def test_progress_snapshot_executor_unavailable_is_encode_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep process-pool failure distinct from progress model failure."""
    monkeypatch.setattr(progress_serialization, "run_boundary", _boundary_unavailable)

    with pytest.raises(ClickHouseEncodeError) as raised:
        await ProgressCodec(JSONSerializer()).encode(_progress())

    _assert_safe_error(
        raised.value,
        operation="progress_encode",
        reason="boundary_unavailable",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("replacement", "reason"),
    [
        pytest.param(RuntimeError(_RAW_ERROR_DETAIL), "model_dump_failed", id="failure"),
        pytest.param(
            asyncio.CancelledError(_RAW_ERROR_DETAIL),
            "model_dump_failed",
            id="dependency-cancellation",
        ),
        pytest.param({}, "model_dump_shape", id="missing-fields"),
        pytest.param((), "model_dump_shape", id="wrong-type"),
    ],
)
async def test_model_dump_failure_or_shape_is_safe(
    monkeypatch: pytest.MonkeyPatch,
    replacement: object,
    reason: str,
) -> None:
    """Reject a failed or contract-drifted Pydantic dump before serialization."""
    source = _progress()

    def patched_dump(*args: object, **kwargs: object) -> object:
        del args, kwargs
        if isinstance(replacement, BaseException):
            raise replacement
        return replacement

    monkeypatch.setattr(source.__class__, "model_dump", patched_dump)

    with pytest.raises(ClickHouseEncodeError) as raised:
        await ProgressCodec(PickleSerializer()).encode(source)

    _assert_safe_error(raised.value, operation="progress_encode", reason=reason)


@pytest.mark.asyncio
async def test_dumped_mapping_hostile_key_is_safe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep exact-key evaluation inside the detached model boundary."""
    source = _progress()
    hostile_mapping = {_ExplodingKey(): "CUSTOM", "meta": None}

    def patched_dump(*args: object, **kwargs: object) -> object:
        del args, kwargs
        return hostile_mapping

    monkeypatch.setattr(source.__class__, "model_dump", patched_dump)

    with pytest.raises(ClickHouseEncodeError) as raised:
        await ProgressCodec(PickleSerializer()).encode(source)

    _assert_safe_error(raised.value, operation="progress_encode", reason="model_dump_shape")


@pytest.mark.asyncio
async def test_snapshot_mapping_dependency_cancellation_is_safe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Classify cancellation inside exact mapping hooks as a shape failure."""
    cancellation = asyncio.CancelledError(_RAW_ERROR_DETAIL)

    def cancelling_snapshot(*args: object, **kwargs: object) -> dict[str, object]:
        del args, kwargs
        raise cancellation

    monkeypatch.setattr(
        progress_serialization,
        "materialize_exact_mapping",
        cancelling_snapshot,
    )

    with pytest.raises(ClickHouseEncodeError) as raised:
        await ProgressCodec(PickleSerializer()).encode(_progress())

    _assert_safe_error(raised.value, operation="progress_encode", reason="model_dump_shape")


@pytest.mark.asyncio
async def test_progress_snapshot_preserves_fatal_signal_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep a process-level model failure distinct from safe dependency failures."""
    source = _progress()
    fatal = _FatalProgressSignal()

    def patched_dump(*args: object, **kwargs: object) -> object:
        del args, kwargs
        raise fatal

    monkeypatch.setattr(source.__class__, "model_dump", patched_dump)

    with pytest.raises(_FatalProgressSignal) as raised:
        await ProgressCodec(PickleSerializer()).encode(source)

    assert raised.value is fatal


@pytest.mark.asyncio
async def test_mutated_invalid_model_fails_before_serializer() -> None:
    """Make Pydantic serialization warnings fatal before any payload write."""
    source = _progress()
    source.state = cast("Any", 1)
    serializer = _RecordingSerializer()

    with pytest.raises(ClickHouseEncodeError) as raised:
        await ProgressCodec(serializer).encode(source)

    _assert_safe_error(raised.value, operation="progress_encode", reason="model_dump_failed")
    assert serializer.dumped == []


@pytest.mark.asyncio
async def test_serializer_load_failure_is_safe() -> None:
    """Hide raw deserializer details without retaining their traceback."""
    codec = ProgressCodec(
        _ScriptedSerializer(load_events=[RuntimeError(_RAW_ERROR_DETAIL)]),
    )

    with pytest.raises(ClickHouseDecodeError) as raised:
        await codec.decode(b"progress")

    _assert_safe_error(
        raised.value,
        operation="progress_decode",
        reason="progress_payload_decode_failed",
    )


@pytest.mark.asyncio
async def test_serializer_load_dependency_cancellation_is_safe() -> None:
    """Classify sync deserializer cancellation as dependency failure."""
    codec = ProgressCodec(
        _ScriptedSerializer(load_events=[asyncio.CancelledError(_RAW_ERROR_DETAIL)]),
    )

    with pytest.raises(ClickHouseDecodeError) as raised:
        await codec.decode(b"progress")

    _assert_safe_error(
        raised.value,
        operation="progress_decode",
        reason="progress_payload_decode_failed",
    )


@pytest.mark.asyncio
async def test_decode_rejects_non_exact_persisted_bytes() -> None:
    """Reject storage type corruption before calling the serializer."""
    serializer = _ScriptedSerializer(load_events=[_progress_mapping()])

    with pytest.raises(ClickHouseDataCorruptionError) as raised:
        await ProgressCodec(serializer).decode(cast("bytes", bytearray(b"progress")))

    _assert_safe_error(
        raised.value,
        operation="progress_decode",
        reason="progress_payload_type",
    )
    assert serializer.loaded == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "decoded",
    [
        pytest.param(None, id="not-mapping"),
        pytest.param({"state": "CUSTOM"}, id="missing-field"),
        pytest.param({**_progress_mapping(), "extra": True}, id="extra-field"),
        pytest.param(_ExplodingMapping(), id="hostile-mapping"),
    ],
)
async def test_decoded_payload_requires_exact_mapping(decoded: object) -> None:
    """Reject mapping shape drift and hostile implementations as corruption."""
    codec = ProgressCodec(_ScriptedSerializer(load_events=[decoded]))

    with pytest.raises(ClickHouseDataCorruptionError) as raised:
        await codec.decode(b"progress")

    _assert_safe_error(
        raised.value,
        operation="progress_decode",
        reason="progress_payload_shape",
    )


@pytest.mark.asyncio
async def test_progress_mapping_executor_unavailable_is_decode_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Do not classify mapping executor failure as malformed progress."""
    monkeypatch.setattr(progress_serialization, "copy_exact_mapping", _boundary_unavailable)
    codec = ProgressCodec(_ScriptedSerializer(load_events=[_progress_mapping()]))

    with pytest.raises(ClickHouseDecodeError) as raised:
        await codec.decode(b"progress")

    _assert_safe_error(
        raised.value,
        operation="progress_decode",
        reason="boundary_unavailable",
    )


@pytest.mark.asyncio
async def test_decoded_mapping_requires_strict_progress_model() -> None:
    """Reject values that Pydantic would otherwise need to coerce."""
    codec = ProgressCodec(
        _ScriptedSerializer(load_events=[_progress_mapping(state=1)]),
    )

    with pytest.raises(ClickHouseDataCorruptionError) as raised:
        await codec.decode(b"progress")

    _assert_safe_error(
        raised.value,
        operation="progress_decode",
        reason="progress_model_invalid",
    )


@pytest.mark.asyncio
async def test_progress_validation_executor_unavailable_is_decode_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Do not classify validation executor failure as corrupt progress."""
    monkeypatch.setattr(progress_serialization, "run_boundary", _boundary_unavailable)
    codec = ProgressCodec(_ScriptedSerializer(load_events=[_progress_mapping()]))

    with pytest.raises(ClickHouseDecodeError) as raised:
        await codec.decode(b"progress")

    _assert_safe_error(
        raised.value,
        operation="progress_decode",
        reason="boundary_unavailable",
    )


@pytest.mark.asyncio
async def test_model_validation_rejects_non_model_return(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fail closed if a future Taskiq validator returns another object."""

    class _WrongProgressModel:
        @classmethod
        def model_validate(cls, candidate: object, *, strict: bool) -> object:
            del cls, candidate, strict
            return object()

    monkeypatch.setattr(progress_serialization, "_PROGRESS_MODEL", _WrongProgressModel)
    codec = ProgressCodec(_ScriptedSerializer(load_events=[_progress_mapping()]))

    with pytest.raises(ClickHouseDataCorruptionError) as raised:
        await codec.decode(b"progress")

    _assert_safe_error(
        raised.value,
        operation="progress_decode",
        reason="progress_model_invalid",
    )


@pytest.mark.asyncio
async def test_model_and_serializer_work_use_executor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep every synchronous model and serializer call off the event loop."""
    event_loop_thread = get_ident()
    model_threads: list[int] = []
    source = _progress(state="CUSTOM", meta={"thread": "placement"})
    original_dump = source.model_dump
    original_validate = progress_serialization._PROGRESS_MODEL.model_validate  # noqa: SLF001

    def observed_dump(*args: object, **kwargs: object) -> dict[str, object]:
        del args, kwargs
        model_threads.append(get_ident())
        return original_dump(mode="python")

    class _ObservedProgressModel:
        @classmethod
        def model_validate(
            cls,
            candidate: object,
            *,
            strict: bool,
        ) -> TaskProgress[Any]:
            del cls
            model_threads.append(get_ident())
            return original_validate(candidate, strict=strict)

    monkeypatch.setattr(source.__class__, "model_dump", observed_dump)
    monkeypatch.setattr(progress_serialization, "_PROGRESS_MODEL", _ObservedProgressModel)
    serializer = _ThreadProbeSerializer()
    codec = ProgressCodec(serializer)

    payload = await codec.encode(source)
    await codec.decode(payload)

    assert len(model_threads) == _PAIR_SIZE
    assert all(thread_id != event_loop_thread for thread_id in model_threads)
    assert len(serializer.thread_ids) == _PAIR_SIZE
    assert all(thread_id != event_loop_thread for thread_id in serializer.thread_ids)


@pytest.mark.asyncio
async def test_encode_submits_one_atomic_model_snapshot_job(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep model dump and exact mapping materialization in one executor job."""
    event_loop = asyncio.get_running_loop()
    original_run_in_executor = event_loop.run_in_executor
    submitted_jobs = 0

    def observed_run_in_executor(
        executor: Executor | None,
        callback: Callable[..., object],
        *args: object,
    ) -> asyncio.Future[object]:
        nonlocal submitted_jobs
        submitted_jobs += 1
        return original_run_in_executor(executor, callback, *args)

    monkeypatch.setattr(event_loop, "run_in_executor", observed_run_in_executor)

    await ProgressCodec(PickleSerializer()).encode(_progress())

    assert submitted_jobs == _PAIR_SIZE


@pytest.mark.asyncio
async def test_encode_cancellation_propagates_without_retry() -> None:
    """Wait for the exact serializer job before propagating cancellation."""
    started = Event()
    release = Event()
    serializer = _BlockingSerializer(started, release)
    task = asyncio.create_task(ProgressCodec(serializer).encode(_progress()))
    try:
        started_in_time = await asyncio.get_running_loop().run_in_executor(
            None,
            started.wait,
            2,
        )
        assert started_in_time
        task.cancel("outer-cancellation")
        await asyncio.sleep(0)
        assert not task.done()
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError) as raised:
        await task

    assert serializer.calls == 1
    assert raised.value.args == ("outer-cancellation",)
