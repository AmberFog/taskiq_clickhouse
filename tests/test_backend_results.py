"""Verify Taskiq result use cases over explicit store and codec ports."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import pytest
from taskiq.result import TaskiqResult

from taskiq_clickhouse import _backend_results as result_operations
from taskiq_clickhouse.exceptions import (
    ClickHouseBackendIOError,
    ClickHouseConfigurationError,
    ClickHouseEncodeError,
    ClickHouseResultNotFoundError,
)


if TYPE_CHECKING:
    from taskiq_clickhouse._result_ports import EncodedResult, StoredResult


@dataclass(frozen=True, slots=True)
class _Encoded:
    result_payload: bytes
    log_payload: bytes


@dataclass(frozen=True, slots=True)
class _Selected:
    result_payload: bytes = b"stored-result"
    log_payload: bytes | None = None


@dataclass(slots=True)
class _FakeCodec:
    events: list[str]
    decoded: TaskiqResult[Any]
    encode_error: BaseException | None = None
    decode_error: BaseException | None = None
    encode_calls: list[TaskiqResult[Any]] = field(default_factory=list)
    decode_calls: list[tuple[bytes, bytes | None]] = field(default_factory=list)

    async def encode(self, task_result: TaskiqResult[Any]) -> EncodedResult:
        """Record and resolve one configured encoding outcome."""
        self.events.append("encode")
        self.encode_calls.append(task_result)
        if self.encode_error is not None:
            raise self.encode_error
        return _Encoded(b"encoded-result", b"encoded-log")

    async def decode(
        self,
        result_payload: bytes,
        log_payload: bytes | None,
    ) -> TaskiqResult[Any]:
        """Record and resolve one configured decoding outcome."""
        self.events.append("decode")
        self.decode_calls.append((result_payload, log_payload))
        if self.decode_error is not None:
            raise self.decode_error
        return self.decoded


@dataclass(slots=True)
class _FakeResultStore:
    events: list[str]
    selected: _Selected | None = field(default_factory=_Selected)
    ready: bool = True
    tombstone_error: BaseException | None = None
    writes: list[tuple[str, bytes, bytes]] = field(default_factory=list)
    readiness_calls: list[str] = field(default_factory=list)
    read_calls: list[tuple[str, bool]] = field(default_factory=list)
    tombstones: list[object] = field(default_factory=list)

    async def write_result(
        self,
        task_id: str,
        result_payload: bytes,
        log_payload: bytes,
    ) -> object:
        """Record one result write."""
        self.events.append("write")
        written = (task_id, result_payload, log_payload)
        self.writes.append(written)
        return written

    async def is_result_ready(self, task_id: str) -> bool:
        """Record and return configured readiness."""
        self.events.append("ready")
        self.readiness_calls.append(task_id)
        return self.ready

    async def read_result_no_log(self, task_id: str) -> StoredResult | None:
        """Record a no-log projection read."""
        self.events.append("read-no-log")
        self.read_calls.append((task_id, False))
        return self.selected

    async def read_result_with_log(self, task_id: str) -> StoredResult | None:
        """Record a with-log projection read."""
        self.events.append("read-with-log")
        self.read_calls.append((task_id, True))
        return self.selected

    async def write_tombstone(self, selected: StoredResult) -> object:
        """Record or fail one targeted tombstone write."""
        self.events.append("tombstone")
        self.tombstones.append(selected)
        if self.tombstone_error is not None:
            raise self.tombstone_error
        return selected


def _task_result(*, log: str | None = "task-log") -> TaskiqResult[Any]:
    return TaskiqResult(
        is_err=False,
        log=log,
        return_value={"value": 1},
        execution_time=0.25,
        labels={"queue": "default"},
        error=None,
    )


async def _run_invalid_task_operation(
    method_name: str,
    store: _FakeResultStore,
    codec: _FakeCodec,
) -> None:
    if method_name == "set_result":
        await result_operations.set_result(store, codec, 1, _task_result())
        return
    if method_name == "is_result_ready":
        await result_operations.is_result_ready(store, 1)
        return
    await result_operations.get_result(
        store,
        codec,
        1,
        with_logs=False,
        keep_results=True,
    )


@pytest.mark.asyncio
async def test_set_result_encodes_before_write() -> None:
    """Never insert a partial result/log serialization."""
    events: list[str] = []
    store = _FakeResultStore(events)
    codec = _FakeCodec(events, _task_result())
    task_result = _task_result()

    await result_operations.set_result(store, codec, "task\x00id", task_result)

    assert events == ["encode", "write"]
    assert codec.encode_calls == [task_result]
    assert store.writes == [("task\x00id", b"encoded-result", b"encoded-log")]


@pytest.mark.asyncio
async def test_set_result_encode_failure_performs_no_write() -> None:
    """Propagate the safe codec failure without touching storage."""
    events: list[str] = []
    store = _FakeResultStore(events)
    encode_error = ClickHouseEncodeError("result_encode", "encode_failed")
    codec = _FakeCodec(events, _task_result(), encode_error=encode_error)

    with pytest.raises(ClickHouseEncodeError) as raised:
        await result_operations.set_result(store, codec, "task", _task_result())

    assert raised.value is encode_error
    assert events == ["encode"]
    assert store.writes == []


@pytest.mark.asyncio
@pytest.mark.parametrize("ready", [False, True])
async def test_readiness_is_metadata_only_and_preserves_boolean(*, ready: bool) -> None:
    """Return store readiness exactly without payload codec work."""
    events: list[str] = []
    store = _FakeResultStore(events, ready=ready)

    observed = await result_operations.is_result_ready(store, "task")

    assert observed is ready
    assert events == ["ready"]
    assert store.readiness_calls == ["task"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("with_logs", "log_payload", "expected_event"),
    [
        pytest.param(False, None, "read-no-log", id="without-log"),
        pytest.param(True, b"stored-log", "read-with-log", id="with-log"),
    ],
)
async def test_read_selects_only_requested_projection(
    *,
    with_logs: bool,
    log_payload: bytes | None,
    expected_event: str,
) -> None:
    """Pass only the requested physical projection to decoding."""
    events: list[str] = []
    selected = _Selected(log_payload=log_payload)
    store = _FakeResultStore(events, selected=selected)
    decoded = _task_result(log=None if not with_logs else "stored-log")
    codec = _FakeCodec(events, decoded)

    observed = await result_operations.get_result(
        store,
        codec,
        "task",
        with_logs=with_logs,
        keep_results=True,
    )

    assert observed is decoded
    assert events == [expected_event, "decode"]
    assert codec.decode_calls == [(b"stored-result", log_payload)]
    assert store.tombstones == []


@pytest.mark.asyncio
async def test_missing_result_is_distinct_and_skips_decode() -> None:
    """Raise Taskiq-compatible absence only for a missing latest selection."""
    events: list[str] = []
    store = _FakeResultStore(events, selected=None)
    codec = _FakeCodec(events, _task_result())

    with pytest.raises(ClickHouseResultNotFoundError, match="not_found") as raised:
        await result_operations.get_result(
            store,
            codec,
            "missing",
            with_logs=False,
            keep_results=True,
        )

    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None
    assert events == ["read-no-log"]
    assert codec.decode_calls == []


@pytest.mark.asyncio
async def test_consume_decodes_then_acknowledges_tombstone() -> None:
    """Return a decoded value only after its selected generation is consumed."""
    events: list[str] = []
    selected = _Selected(log_payload=b"stored-log")
    store = _FakeResultStore(events, selected=selected)
    decoded = _task_result(log="stored-log")
    codec = _FakeCodec(events, decoded)

    observed = await result_operations.get_result(
        store,
        codec,
        "task",
        with_logs=True,
        keep_results=False,
    )

    assert observed is decoded
    assert events == ["read-with-log", "decode", "tombstone"]
    assert store.tombstones == [selected]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_stage", ["decode", "tombstone"])
async def test_consume_failure_stops_at_failed_stage(failure_stage: str) -> None:
    """Do not consume corrupt data or claim success before acknowledgement."""
    events: list[str] = []
    safe_error = ClickHouseBackendIOError("result_read", "database_error")
    store = _FakeResultStore(
        events,
        tombstone_error=safe_error if failure_stage == "tombstone" else None,
    )
    codec = _FakeCodec(
        events,
        _task_result(),
        decode_error=safe_error if failure_stage == "decode" else None,
    )

    with pytest.raises(ClickHouseBackendIOError) as raised:
        await result_operations.get_result(
            store,
            codec,
            "task",
            with_logs=False,
            keep_results=False,
        )

    assert raised.value is safe_error
    expected = ["read-no-log", "decode"]
    if failure_stage == "tombstone":
        expected.append("tombstone")
    assert events == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("method_name", ["set_result", "is_result_ready", "get_result"])
async def test_invalid_task_id_fails_before_codec_or_store(method_name: str) -> None:
    """Reject the runtime type violation at the use-case boundary."""
    events: list[str] = []
    store = _FakeResultStore(events)
    codec = _FakeCodec(events, _task_result())

    with pytest.raises(ClickHouseConfigurationError, match="invalid_task_id"):
        await _run_invalid_task_operation(method_name, store, codec)

    assert events == []


@pytest.mark.asyncio
async def test_invalid_with_logs_fails_before_read_or_decode() -> None:
    """Require an exact bool before choosing a storage projection."""
    events: list[str] = []
    store = _FakeResultStore(events)
    codec = _FakeCodec(events, _task_result())

    with pytest.raises(ClickHouseConfigurationError, match="invalid_with_logs"):
        await result_operations.get_result(
            store,
            codec,
            "task",
            with_logs=1,
            keep_results=True,
        )

    assert events == []


@pytest.mark.asyncio
async def test_cancellation_stops_before_tombstone() -> None:
    """Propagate codec cancellation and leave the generation untouched."""
    events: list[str] = []
    cancellation = asyncio.CancelledError()
    store = _FakeResultStore(events)
    codec = _FakeCodec(events, _task_result(), decode_error=cancellation)

    with pytest.raises(asyncio.CancelledError) as raised:
        await result_operations.get_result(
            store,
            codec,
            "task",
            with_logs=False,
            keep_results=False,
        )

    assert raised.value is cancellation
    assert events == ["read-no-log", "decode"]
    assert store.tombstones == []
