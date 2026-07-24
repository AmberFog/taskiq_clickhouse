"""Verify Taskiq progress use cases over explicit store and codec ports."""

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import pytest
from taskiq.depends.progress_tracker import TaskProgress, TaskState

from taskiq_clickhouse import _backend_progress as progress_operations
from taskiq_clickhouse.exceptions import (
    ClickHouseBackendIOError,
    ClickHouseConfigurationError,
    ClickHouseDataCorruptionError,
    ClickHouseEncodeError,
)


if TYPE_CHECKING:
    from collections.abc import Awaitable


@dataclass(frozen=True, slots=True)
class _SelectedProgress:
    progress_payload: bytes = b"stored-progress"


@dataclass(slots=True)
class _FakeCodec:
    decoded: TaskProgress[Any]
    encode_error: Exception | None = None
    decode_error: Exception | None = None
    encode_calls: list[TaskProgress[Any]] = field(default_factory=list)
    decode_calls: list[bytes] = field(default_factory=list)

    async def encode(self, progress: TaskProgress[Any]) -> bytes:
        """Record and resolve one configured encoding outcome."""
        self.encode_calls.append(progress)
        if self.encode_error is not None:
            raise self.encode_error
        return b"encoded-progress"

    async def decode(self, progress_payload: bytes) -> TaskProgress[Any]:
        """Record and resolve one configured decoding outcome."""
        self.decode_calls.append(progress_payload)
        if self.decode_error is not None:
            raise self.decode_error
        return self.decoded


@dataclass(slots=True)
class _FakeProgressStore:
    selected: _SelectedProgress | None = field(default_factory=_SelectedProgress)
    write_error: Exception | None = None
    read_error: Exception | None = None
    writes: list[tuple[str, bytes]] = field(default_factory=list)
    reads: list[str] = field(default_factory=list)

    async def write_progress(
        self,
        task_id: str,
        progress_payload: bytes,
    ) -> object:
        """Record or fail one progress write."""
        self.writes.append((task_id, progress_payload))
        if self.write_error is not None:
            raise self.write_error
        return progress_payload

    async def read_progress(self, task_id: str) -> _SelectedProgress | None:
        """Record or fail one progress read."""
        self.reads.append(task_id)
        if self.read_error is not None:
            raise self.read_error
        return self.selected


def _progress() -> TaskProgress[Any]:
    return TaskProgress(state=TaskState.STARTED, meta={"completed": 2, "total": 5})


@pytest.mark.asyncio
async def test_set_progress_encodes_before_one_write() -> None:
    """Never persist a partial or unencoded progress model."""
    progress = _progress()
    codec = _FakeCodec(progress)
    store = _FakeProgressStore()

    await progress_operations.set_progress(store, codec, "task\x00id", progress)

    assert codec.encode_calls == [progress]
    assert store.writes == [("task\x00id", b"encoded-progress")]


@pytest.mark.asyncio
async def test_set_progress_encode_failure_performs_no_write() -> None:
    """Propagate safe encoding failure before touching the store."""
    progress = _progress()
    encode_error = ClickHouseEncodeError("progress_encode", "model_dump_failed")
    codec = _FakeCodec(progress, encode_error=encode_error)
    store = _FakeProgressStore()

    with pytest.raises(ClickHouseEncodeError) as raised:
        await progress_operations.set_progress(store, codec, "task", progress)

    assert raised.value is encode_error
    assert store.writes == []


@pytest.mark.asyncio
async def test_missing_progress_returns_none_without_decoding() -> None:
    """Preserve Taskiq's missing-progress contract without hiding failures."""
    progress = _progress()
    codec = _FakeCodec(progress)
    store = _FakeProgressStore(selected=None)

    observed = await progress_operations.get_progress(store, codec, "task")

    assert observed is None
    assert store.reads == ["task"]
    assert codec.decode_calls == []


@pytest.mark.asyncio
async def test_repeated_reads_are_non_consuming() -> None:
    """Decode each selected update without a tombstone or store mutation."""
    progress = _progress()
    codec = _FakeCodec(progress)
    store = _FakeProgressStore()

    first = await progress_operations.get_progress(store, codec, "task")
    second = await progress_operations.get_progress(store, codec, "task")

    assert first is progress
    assert second is progress
    assert store.reads == ["task", "task"]
    assert store.writes == []
    assert codec.decode_calls == [b"stored-progress", b"stored-progress"]


@pytest.mark.asyncio
async def test_read_failure_is_not_reduced_to_missing() -> None:
    """Propagate storage failures instead of returning a false absence."""
    progress = _progress()
    read_error = ClickHouseBackendIOError("progress_read", "query_failed")
    store = _FakeProgressStore(read_error=read_error)

    with pytest.raises(ClickHouseBackendIOError) as raised:
        await progress_operations.get_progress(store, _FakeCodec(progress), "task")

    assert raised.value is read_error


@pytest.mark.asyncio
async def test_decode_corruption_is_not_reduced_to_missing() -> None:
    """Propagate a safe corruption error for a physically selected payload."""
    progress = _progress()
    decode_error = ClickHouseDataCorruptionError("progress_decode", "progress_model_invalid")
    codec = _FakeCodec(progress, decode_error=decode_error)

    with pytest.raises(ClickHouseDataCorruptionError) as raised:
        await progress_operations.get_progress(_FakeProgressStore(), codec, "task")

    assert raised.value is decode_error


@pytest.mark.asyncio
async def test_write_failure_is_propagated_after_single_attempt() -> None:
    """Leave storage retry and acknowledgement policy inside the store."""
    progress = _progress()
    write_error = ClickHouseBackendIOError("progress_write", "insert_failed")
    store = _FakeProgressStore(write_error=write_error)

    with pytest.raises(ClickHouseBackendIOError) as raised:
        await progress_operations.set_progress(store, _FakeCodec(progress), "task", progress)

    assert raised.value is write_error
    assert store.writes == [("task", b"encoded-progress")]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("operation", "expected_operation"),
    [
        pytest.param("write", "progress_write", id="write"),
        pytest.param("read", "progress_read", id="read"),
    ],
)
async def test_invalid_task_id_fails_before_codec_or_store(
    operation: str,
    expected_operation: str,
) -> None:
    """Reject non-text task ids at the use-case boundary."""
    progress = _progress()
    codec = _FakeCodec(progress)
    store = _FakeProgressStore()

    invocation: Awaitable[object]
    if operation == "write":
        invocation = progress_operations.set_progress(store, codec, 1, progress)
    else:
        invocation = progress_operations.get_progress(store, codec, 1)
    with pytest.raises(ClickHouseConfigurationError) as raised:
        await invocation

    assert raised.value.operation == expected_operation
    assert raised.value.reason == "invalid_task_id"
    assert codec.encode_calls == []
    assert codec.decode_calls == []
    assert store.writes == []
    assert store.reads == []
