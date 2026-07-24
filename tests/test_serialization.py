"""Verify the isolated Taskiq result serialization and model boundary."""

import asyncio
from dataclasses import FrozenInstanceError
from typing import Any, cast
import warnings

import pytest
from taskiq.abc.serializer import TaskiqSerializer
from taskiq.result import TaskiqResult
from taskiq.serializers.json_serializer import JSONSerializer
from taskiq.serializers.pickle import PickleSerializer

import taskiq_clickhouse._result_model as result_model_module
import taskiq_clickhouse._serialization as serialization_module
from taskiq_clickhouse._serialization import EncodedResult, ResultCodec
import taskiq_clickhouse._serializer_boundary as serializer_boundary_module
from taskiq_clickhouse.exceptions import (
    ClickHouseDataCorruptionError,
    ClickHouseDecodeError,
    ClickHouseEncodeError,
)
from tests.factories.results import TaskiqResultFactory
from tests.serializer_testkit import (
    SERIALIZER_FAILURE_DETAIL as _RAW_ERROR_DETAIL,
    BytesSubclass as _BytesSubclass,
    ExplodingMapping as _ExplodingMapping,
    RecordingSerializer as _RecordingSerializer,
    ScriptedSerializer as _ScriptedSerializer,
    assert_safe_error as _assert_safe_error,
    boundary_unavailable as _boundary_unavailable,
)


_RESULT_FIELDS = frozenset(
    ("is_err", "return_value", "execution_time", "labels", "error"),
)
_PAIR_SIZE = 2


class _HostileReturnSerializer(TaskiqSerializer):
    """Return a forged value without inspecting its hostile attributes."""

    def dumpb(self, candidate: object) -> bytes:
        del candidate
        return cast("bytes", _HostileClassAccess())

    def loadb(self, payload: bytes) -> object:
        del payload
        return None


class _StringSubclass(str):
    """Distinguish exact persisted strings from coercible subclasses."""

    __slots__ = ()


class _HostileClassAccess:
    """Raise if a boundary reads the forgeable ``__class__`` attribute."""

    def __getattribute__(self, attribute: str) -> object:
        if attribute == "__class__":
            raise RuntimeError(_RAW_ERROR_DETAIL)
        return object.__getattribute__(self, attribute)


class _ExplodingKey:
    """Collide with a result key and fail during exact-set equality."""

    def __hash__(self) -> int:
        return hash("is_err")

    def __eq__(self, candidate: object) -> bool:
        del candidate
        raise RuntimeError(_RAW_ERROR_DETAIL)


def _result_mapping(**replacements: object) -> dict[str, object]:
    return {
        "is_err": False,
        "return_value": {"answer": 42},
        "execution_time": 1.25,
        "labels": {"queue": "tests"},
        "error": None,
        **replacements,
    }


def _hostile_key_mapping() -> dict[object, object]:
    return {
        _ExplodingKey(): False,
        "return_value": {"answer": 42},
        "execution_time": 1.25,
        "labels": {"queue": "tests"},
        "error": None,
    }


def test_encoded_result_is_frozen_and_payload_safe_in_repr() -> None:
    """Keep encoded payloads immutable and absent from diagnostic reprs."""
    encoded = EncodedResult(b"private-result", b"private-log")

    assert "private" not in repr(encoded)
    with pytest.raises(FrozenInstanceError):
        encoded.result_payload = b"replacement"  # type: ignore[misc]


@pytest.mark.asyncio
async def test_json_round_trip_separates_result_and_optional_log() -> None:
    """Round-trip the default JSON graph and omit log decoding on demand."""
    codec = ResultCodec(JSONSerializer(default=None, ensure_ascii=True))
    source = TaskiqResultFactory.build(return_value={"items": [1, "two", None]})

    encoded = await codec.encode(source)
    with_log = await codec.decode(encoded.result_payload, encoded.log_payload)
    without_log = await codec.decode(encoded.result_payload, None)

    assert with_log == source
    assert without_log.model_dump(exclude={"log"}) == source.model_dump(exclude={"log"})
    assert without_log.log is None


@pytest.mark.asyncio
async def test_default_json_applies_standard_json_normalization() -> None:
    """Pin accepted Python values to their standard JSON round-trip shape."""
    codec = ResultCodec(JSONSerializer(default=None, ensure_ascii=True))
    source = TaskiqResultFactory.build(
        return_value={
            "sequence": (1, 2),
            "by_id": {7: "seven"},
            "text": "Málaga",
        },
    )

    encoded = await codec.encode(source)
    decoded = await codec.decode(encoded.result_payload, encoded.log_payload)

    assert b"M\\u00e1laga" in encoded.result_payload
    assert decoded.return_value == {
        "sequence": [1, 2],
        "by_id": {"7": "seven"},
        "text": "Málaga",
    }


@pytest.mark.asyncio
async def test_json_round_trip_reconstructs_taskiq_error() -> None:
    """Exercise Taskiq's global exception serializer and validator state."""
    source = TaskiqResult[Any](
        is_err=True,
        log="failure-log",
        return_value=None,
        execution_time=0.5,
        labels={"queue": "tests"},
        error=ValueError("expected-task-error"),
    )
    codec = ResultCodec(JSONSerializer())

    encoded = await codec.encode(source)
    decoded = await codec.decode(encoded.result_payload, encoded.log_payload)

    assert decoded.is_err
    assert isinstance(decoded.error, ValueError)
    assert str(decoded.error) == "expected-task-error"
    assert decoded.log == source.log


@pytest.mark.asyncio
async def test_custom_serializer_receives_mapping_and_log_as_two_values() -> None:
    """Never place log inside the serialized result mapping."""
    serializer = _RecordingSerializer()
    codec = ResultCodec(serializer)
    source = TaskiqResultFactory.build(return_value={"custom": True}, log="separate-log")

    encoded = await codec.encode(source)
    decoded = await codec.decode(encoded.result_payload, encoded.log_payload)

    assert len(serializer.dumped) == _PAIR_SIZE
    assert isinstance(serializer.dumped[0], dict)
    assert frozenset(serializer.dumped[0]) == _RESULT_FIELDS
    assert "log" not in serializer.dumped[0]
    assert serializer.dumped[1] == "separate-log"
    assert serializer.loaded == [encoded.result_payload, encoded.log_payload]
    assert decoded == source


@pytest.mark.asyncio
async def test_unsupported_json_graph_is_safe_encode_error() -> None:
    """Reject Python-mode sets before storage without retaining JSON errors."""
    codec = ResultCodec(JSONSerializer(default=None, ensure_ascii=True))

    with pytest.raises(ClickHouseEncodeError) as raised:
        await codec.encode(TaskiqResultFactory.build(return_value={"unsupported": {1, 2}}))

    _assert_safe_error(
        raised.value,
        operation="result_encode",
        reason="result_payload_encode_failed",
    )


@pytest.mark.asyncio
async def test_result_snapshot_executor_unavailable_is_encode_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep process-pool failure distinct from model snapshot failure."""
    monkeypatch.setattr(serialization_module, "run_boundary", _boundary_unavailable)

    with pytest.raises(ClickHouseEncodeError) as raised:
        await ResultCodec(JSONSerializer()).encode(TaskiqResultFactory.build())

    _assert_safe_error(
        raised.value,
        operation="result_encode",
        reason="boundary_unavailable",
    )


@pytest.mark.asyncio
async def test_result_serializer_executor_unavailable_is_encode_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Classify serializer pool failure without blaming the input graph."""
    monkeypatch.setattr(serializer_boundary_module, "run_boundary", _boundary_unavailable)

    with pytest.raises(ClickHouseEncodeError) as raised:
        await ResultCodec(JSONSerializer()).encode(TaskiqResultFactory.build())

    _assert_safe_error(
        raised.value,
        operation="result_encode",
        reason="boundary_unavailable",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("events", "reason"),
    [
        ([RuntimeError(_RAW_ERROR_DETAIL)], "result_payload_encode_failed"),
        ([asyncio.CancelledError(_RAW_ERROR_DETAIL)], "result_payload_encode_failed"),
        ([b"result", RuntimeError(_RAW_ERROR_DETAIL)], "log_payload_encode_failed"),
    ],
    ids=("result-error", "result-cancelled-signal", "log-error"),
)
async def test_serializer_dump_failures_have_no_raw_context(
    events: list[object],
    reason: str,
) -> None:
    """Translate failures from either separate dump outside the raw handler."""
    codec = ResultCodec(_ScriptedSerializer(dump_events=events))

    with pytest.raises(ClickHouseEncodeError) as raised:
        await codec.encode(TaskiqResultFactory.build())

    _assert_safe_error(raised.value, operation="result_encode", reason=reason)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("events", "reason"),
    [
        ([_BytesSubclass(b"result")], "result_payload_not_bytes"),
        ([b"result", bytearray(b"log")], "log_payload_not_bytes"),
    ],
    ids=("result-bytes-subclass", "log-bytearray"),
)
async def test_serializer_dump_requires_exact_bytes(
    events: list[object],
    reason: str,
) -> None:
    """Reject byte-like values and bytes subclasses without coercion."""
    codec = ResultCodec(_ScriptedSerializer(dump_events=events))

    with pytest.raises(ClickHouseEncodeError) as raised:
        await codec.encode(TaskiqResultFactory.build())

    _assert_safe_error(raised.value, operation="result_encode", reason=reason)


@pytest.mark.asyncio
async def test_exact_type_check_ignores_hostile_class_attribute() -> None:
    """Reject a forged serializer value without reading its ``__class__``."""
    codec = ResultCodec(_HostileReturnSerializer())

    with pytest.raises(ClickHouseEncodeError) as raised:
        await codec.encode(TaskiqResultFactory.build())

    _assert_safe_error(
        raised.value,
        operation="result_encode",
        reason="result_payload_not_bytes",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "replacement",
    [RuntimeError(_RAW_ERROR_DETAIL), asyncio.CancelledError(_RAW_ERROR_DETAIL), {}, ()],
    ids=("runtime-error", "cancelled-signal", "empty-mapping", "tuple"),
)
async def test_model_dump_failure_or_shape_is_safe(
    monkeypatch: pytest.MonkeyPatch,
    replacement: object,
) -> None:
    """Protect model extraction and require its exact no-log field set."""
    source = TaskiqResultFactory.build()

    def patched_dump(*args: object, **kwargs: object) -> dict[str, object]:
        del args, kwargs
        if isinstance(replacement, BaseException):
            raise replacement
        return cast("dict[str, object]", replacement)

    monkeypatch.setattr(source.__class__, "model_dump", patched_dump)
    expected_reason = "model_dump_failed" if isinstance(replacement, BaseException) else "model_dump_shape"

    with pytest.raises(ClickHouseEncodeError) as raised:
        await ResultCodec(PickleSerializer()).encode(source)

    _assert_safe_error(raised.value, operation="result_encode", reason=expected_reason)


@pytest.mark.asyncio
async def test_unexpected_snapshot_failure_is_redacted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep an unexpected model-boundary failure outside the public chain."""
    raw_detail = _RAW_ERROR_DETAIL

    def fail_snapshot(_task_result: TaskiqResult[Any]) -> object:
        raise RuntimeError(raw_detail)

    monkeypatch.setattr(
        serialization_module,
        "capture_snapshot",
        fail_snapshot,
    )

    with pytest.raises(ClickHouseEncodeError) as raised:
        await ResultCodec(PickleSerializer()).encode(TaskiqResultFactory.build())

    _assert_safe_error(
        raised.value,
        operation="result_encode",
        reason="model_dump_failed",
    )
    assert raw_detail not in str(raised.value)


@pytest.mark.asyncio
async def test_model_dump_hostile_key_hash_is_safe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep exact-key evaluation and indexed copy inside the safe boundary."""
    source = TaskiqResultFactory.build()
    dumped = _hostile_key_mapping()

    def patched_dump(*args: object, **kwargs: object) -> dict[object, object]:
        del args, kwargs
        return dumped

    monkeypatch.setattr(source.__class__, "model_dump", patched_dump)

    with pytest.raises(ClickHouseEncodeError) as raised:
        await ResultCodec(PickleSerializer()).encode(source)

    _assert_safe_error(
        raised.value,
        operation="result_encode",
        reason="model_dump_shape",
    )


@pytest.mark.asyncio
async def test_snapshot_mapping_dependency_cancellation_is_a_shape_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Use the same stage-aware reason as the progress snapshot boundary."""
    cancellation = asyncio.CancelledError(_RAW_ERROR_DETAIL)

    def cancelling_snapshot(*args: object, **kwargs: object) -> dict[str, object]:
        del args, kwargs
        raise cancellation

    monkeypatch.setattr(
        result_model_module,
        "materialize_exact_mapping",
        cancelling_snapshot,
    )

    with pytest.raises(ClickHouseEncodeError) as raised:
        await ResultCodec(PickleSerializer()).encode(TaskiqResultFactory.build())

    _assert_safe_error(
        raised.value,
        operation="result_encode",
        reason="model_dump_shape",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("forged_log", "expected_reason"),
    [
        pytest.param(..., "model_dump_shape", id="missing"),
        pytest.param(1, "log_model_invalid", id="wrong-type"),
    ],
)
async def test_forged_result_log_never_leaks_model_errors(
    forged_log: object,
    expected_reason: str,
) -> None:
    """Translate a missing or non-string log in the atomic model snapshot."""

    class _ForgedResult:
        def model_dump(self, **kwargs: object) -> dict[str, object]:
            del kwargs
            mapping = _result_mapping()
            if forged_log is not ...:
                mapping["log"] = forged_log
            return mapping

    forged = _ForgedResult()

    with pytest.raises(ClickHouseEncodeError) as raised:
        await ResultCodec(PickleSerializer()).encode(cast("TaskiqResult[Any]", forged))

    _assert_safe_error(
        raised.value,
        operation="result_encode",
        reason=expected_reason,
    )


@pytest.mark.asyncio
async def test_result_and_log_are_encoded_from_one_model_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Prevent a mutable result from combining fields observed at two times."""
    source = TaskiqResultFactory.build(log="snapshot-log")
    original_dump = source.model_dump

    def mutating_dump(*args: object, **kwargs: object) -> dict[str, Any]:
        del args, kwargs
        snapshot = original_dump(mode="python")
        source.log = "later-log"
        return snapshot

    monkeypatch.setattr(source.__class__, "model_dump", mutating_dump)
    serializer = JSONSerializer()

    encoded = await ResultCodec(serializer).encode(source)

    assert serializer.loadb(encoded.log_payload) == "snapshot-log"
    assert source.log == "later-log"


@pytest.mark.asyncio
async def test_corrupt_log_is_ignored_only_on_physical_no_log_path() -> None:
    """Do not receive or decode log bytes unless the caller fetched the column."""
    codec = ResultCodec(JSONSerializer())
    encoded = await codec.encode(TaskiqResultFactory.build(log="healthy"))

    without_log = await codec.decode(encoded.result_payload, None)

    assert without_log.log is None
    with pytest.raises(ClickHouseDecodeError) as raised:
        await codec.decode(encoded.result_payload, b"{corrupt-log")
    _assert_safe_error(
        raised.value,
        operation="result_decode",
        reason="log_payload_decode_failed",
    )


@pytest.mark.asyncio
async def test_malformed_result_payload_is_safe_decode_error() -> None:
    """Hide raw JSON decoding details for malformed persisted bytes."""
    with pytest.raises(ClickHouseDecodeError) as raised:
        await ResultCodec(JSONSerializer()).decode(b"{malformed-result", None)

    _assert_safe_error(
        raised.value,
        operation="result_decode",
        reason="result_payload_decode_failed",
    )


@pytest.mark.asyncio
async def test_result_serializer_executor_unavailable_is_decode_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Classify serializer pool failure without claiming stored corruption."""
    monkeypatch.setattr(serializer_boundary_module, "run_boundary", _boundary_unavailable)

    with pytest.raises(ClickHouseDecodeError) as raised:
        await ResultCodec(JSONSerializer()).decode(b"result", None)

    _assert_safe_error(
        raised.value,
        operation="result_decode",
        reason="boundary_unavailable",
    )


@pytest.mark.asyncio
async def test_result_mapping_executor_unavailable_is_decode_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Do not classify mapping executor failure as a malformed row."""
    monkeypatch.setattr(serialization_module, "copy_exact_mapping", _boundary_unavailable)
    codec = ResultCodec(_ScriptedSerializer(load_events=[_result_mapping()]))

    with pytest.raises(ClickHouseDecodeError) as raised:
        await codec.decode(b"result", None)

    _assert_safe_error(
        raised.value,
        operation="result_decode",
        reason="boundary_unavailable",
    )


@pytest.mark.asyncio
async def test_result_validation_executor_unavailable_is_decode_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Do not classify model-validation executor failure as corrupt data."""
    monkeypatch.setattr(serialization_module, "run_boundary", _boundary_unavailable)
    codec = ResultCodec(_ScriptedSerializer(load_events=[_result_mapping()]))

    with pytest.raises(ClickHouseDecodeError) as raised:
        await codec.decode(b"result", None)

    _assert_safe_error(
        raised.value,
        operation="result_decode",
        reason="boundary_unavailable",
    )


@pytest.mark.asyncio
async def test_mutated_invalid_model_is_rejected_without_warning_or_dump() -> None:
    """Do not log or persist a mutable Taskiq model that cannot round-trip."""
    source = TaskiqResultFactory.build()
    source.labels = cast("dict[str, Any]", "TOP_SECRET_LABEL")
    serializer = _RecordingSerializer()

    with (
        warnings.catch_warnings(record=True) as caught,
        pytest.raises(ClickHouseEncodeError) as raised,
    ):
        await ResultCodec(serializer).encode(source)

    _assert_safe_error(
        raised.value,
        operation="result_encode",
        reason="model_dump_failed",
    )
    assert not caught
    assert not serializer.dumped


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("decoded", "expected_reason"),
    [
        (None, "result_payload_shape"),
        ({"is_err": False}, "result_payload_shape"),
        ({**_result_mapping(), "extra": True}, "result_payload_shape"),
        (_ExplodingMapping(), "result_payload_shape"),
        (_result_mapping(is_err=1), "result_model_invalid"),
    ],
    ids=("not-mapping", "missing-fields", "extra-field", "hostile-mapping", "invalid-field"),
)
async def test_decoded_result_requires_exact_keys_and_strict_model(
    decoded: object,
    expected_reason: str,
) -> None:
    """Reject non-mappings, key drift, hostile mappings and coercible models."""
    codec = ResultCodec(_ScriptedSerializer(load_events=[decoded]))

    with pytest.raises(ClickHouseDataCorruptionError) as raised:
        await codec.decode(b"result", None)

    _assert_safe_error(
        raised.value,
        operation="result_decode",
        reason=expected_reason,
    )


@pytest.mark.asyncio
async def test_decoded_mapping_hostile_key_hash_is_safe() -> None:
    """Translate failures from copied mapping key evaluation without context."""
    decoded = _hostile_key_mapping()
    codec = ResultCodec(_ScriptedSerializer(load_events=[decoded]))

    with pytest.raises(ClickHouseDataCorruptionError) as raised:
        await codec.decode(b"result", None)

    _assert_safe_error(
        raised.value,
        operation="result_decode",
        reason="result_payload_shape",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("decoded_log", "accepted"),
    [("log", True), (None, True), (1, False), (_StringSubclass("log"), False)],
    ids=("string", "none", "integer", "string-subclass"),
)
async def test_decoded_log_requires_exact_string_or_none(
    decoded_log: object,
    *,
    accepted: bool,
) -> None:
    """Reject log coercion while accepting the two frozen native values."""
    codec = ResultCodec(
        _ScriptedSerializer(load_events=[_result_mapping(), decoded_log]),
    )
    if accepted:
        result = await codec.decode(b"result", b"log")
        assert result.log == decoded_log
        return
    with pytest.raises(ClickHouseDataCorruptionError) as raised:
        await codec.decode(b"result", b"log")
    _assert_safe_error(
        raised.value,
        operation="result_decode",
        reason="log_payload_shape",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("result_payload", "log_payload", "reason"),
    [
        (bytearray(b"result"), None, "result_payload_type"),
        (b"result", bytearray(b"log"), "log_payload_type"),
    ],
    ids=("result-bytearray", "log-bytearray"),
)
async def test_decode_requires_exact_persisted_bytes(
    result_payload: object,
    log_payload: object,
    reason: str,
) -> None:
    """Reject corrupt storage types before invoking a custom serializer."""
    serializer = _ScriptedSerializer(load_events=[_result_mapping()])

    with pytest.raises(ClickHouseDataCorruptionError) as raised:
        await ResultCodec(serializer).decode(
            cast("bytes", result_payload),
            cast("bytes | None", log_payload),
        )

    _assert_safe_error(raised.value, operation="result_decode", reason=reason)


@pytest.mark.asyncio
async def test_no_log_decode_invokes_serializer_once() -> None:
    """Make absence of log bytes observable as absence of a second load call."""
    serializer = _ScriptedSerializer(load_events=[_result_mapping()])

    result = await ResultCodec(serializer).decode(b"result", None)

    assert result.log is None
    assert serializer.loaded == [b"result"]


@pytest.mark.asyncio
async def test_model_validation_rejects_non_model_return(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fail closed if a future Taskiq validation surface returns another type."""

    class _WrongResultModel:
        @classmethod
        def model_validate(cls, candidate: object, *, strict: bool) -> object:
            del cls, candidate, strict
            return object()

    monkeypatch.setattr(result_model_module, "_RESULT_MODEL", _WrongResultModel)
    codec = ResultCodec(_ScriptedSerializer(load_events=[_result_mapping()]))

    with pytest.raises(ClickHouseDataCorruptionError) as raised:
        await codec.decode(b"result", None)

    _assert_safe_error(
        raised.value,
        operation="result_decode",
        reason="result_model_invalid",
    )


@pytest.mark.asyncio
async def test_model_validation_dependency_cancellation_is_corruption(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Do not confuse a synchronous model hook signal with caller cancellation."""

    class _CancellingResultModel:
        @classmethod
        def model_validate(cls, candidate: object, *, strict: bool) -> object:
            del cls, candidate, strict
            raise asyncio.CancelledError(_RAW_ERROR_DETAIL)

    monkeypatch.setattr(result_model_module, "_RESULT_MODEL", _CancellingResultModel)
    codec = ResultCodec(_ScriptedSerializer(load_events=[_result_mapping()]))

    with pytest.raises(ClickHouseDataCorruptionError) as raised:
        await codec.decode(b"result", None)

    _assert_safe_error(
        raised.value,
        operation="result_decode",
        reason="result_model_invalid",
    )
