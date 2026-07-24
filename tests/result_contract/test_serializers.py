"""Verify the complete public result serialization contract."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import json
from threading import Event, Lock
from typing import TYPE_CHECKING

import pytest
from taskiq.result import result as taskiq_result_module
from taskiq.serialization import ExceptionRepr, prepare_exception
from taskiq.serializers.json_serializer import JSONSerializer
from taskiq.serializers.pickle import PickleSerializer

from taskiq_clickhouse._serialization import ResultCodec
from taskiq_clickhouse.exceptions import ClickHouseDataCorruptionError, ClickHouseDecodeError, ClickHouseEncodeError
from tests.result_contract.assertions import assert_safe_public_error
from tests.result_contract.backend_actions import CaptureGateway, run_parallel_error_writes
from tests.result_contract.models import context_error_result, error_result, python_only_graph, success_result
from tests.result_contract.serializer_cases import FailingLoadSerializer


if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import Any

    from taskiq.result import TaskiqResult


_HOSTILE_DETAIL = "payload=PRIVATE_TASK_PAYLOAD token=PRIVATE_TOKEN"
_LARGE_LOG = "log-chunk-" * 220_000
_PARALLEL_WRITERS = 4
_ASSERTION_TIMEOUT = 2.0


class _ModelConversionProbe:
    """Hold one conversion while concurrent public writes remain submitted."""

    def __init__(
        self,
        original: Callable[..., object],
    ) -> None:
        """Create explicit entered and release synchronization signals."""
        self._original = original
        self._lock = Lock()
        self._active = 0
        self.entered = Event()
        self.release = Event()
        self.max_active = 0

    def __call__(self, *args: object, **kwargs: object) -> object:
        """Measure overlap until the controlling test releases conversion."""
        with self._lock:
            self._active += 1
            self.max_active = max(self.max_active, self._active)
        self.entered.set()
        try:
            self.release.wait()
            return self._original(*args, **kwargs)
        finally:
            with self._lock:
                self._active -= 1


@pytest.mark.asyncio
async def test_success_round_trip_preserves_every_field_and_source_model() -> None:
    """Keep the caller's complete Taskiq model unchanged during encoding."""
    source = success_result(return_value={"mutable": [1, 2, 3]})
    snapshot = source.model_copy(deep=True)
    codec = ResultCodec(JSONSerializer())

    encoded = await codec.encode(source)
    decoded = await codec.decode(encoded.result_payload, encoded.log_payload)

    assert source == snapshot
    assert decoded == snapshot
    assert decoded is not source
    assert decoded.return_value is not source.return_value


@pytest.mark.asyncio
@pytest.mark.parametrize("source", [error_result(), context_error_result()])
async def test_builtin_custom_cause_and_context_round_trip(source: TaskiqResult[Any]) -> None:
    """Reconstruct module-level task errors and their exception chains."""
    codec = ResultCodec(JSONSerializer())

    encoded = await codec.encode(source)
    decoded = await codec.decode(encoded.result_payload, encoded.log_payload)

    assert source.error is not None
    assert decoded.error is not None
    assert type(decoded.error) is type(source.error)
    assert str(decoded.error) == str(source.error)
    assert type(decoded.error.__cause__) is type(source.error.__cause__)
    assert str(decoded.error.__cause__) == str(source.error.__cause__)
    assert type(decoded.error.__context__) is type(source.error.__context__)
    assert str(decoded.error.__context__) == str(source.error.__context__)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "log",
    [
        pytest.param(None, id="none"),
        pytest.param("", id="empty"),
        pytest.param(_LARGE_LOG, id="multi-megabyte"),
    ],
)
async def test_none_empty_and_multi_megabyte_logs_round_trip(log: str | None) -> None:
    """Preserve all supported log shapes through the independent payload."""
    codec = ResultCodec(JSONSerializer())

    encoded = await codec.encode(success_result(log=log))
    decoded = await codec.decode(encoded.result_payload, encoded.log_payload)

    assert decoded.log == log
    assert "log" not in json.loads(encoded.result_payload)


@pytest.mark.asyncio
async def test_pickle_preserves_complete_python_mode_graph() -> None:
    """Keep bytes, datetime, set and custom values only on the Pickle path."""
    graph = python_only_graph()
    codec = ResultCodec(PickleSerializer())

    encoded = await codec.encode(success_result(return_value=graph))
    decoded = await codec.decode(encoded.result_payload, encoded.log_payload)

    assert decoded.return_value == graph
    assert type(decoded.return_value["bytes"]) is bytes
    assert type(decoded.return_value["members"]) is set
    assert type(decoded.return_value["coordinates"]) is tuple
    assert type(decoded.return_value["custom"]) is type(graph["custom"])


@pytest.mark.asyncio
@pytest.mark.parametrize("unsupported", [b"bytes", {"set"}, python_only_graph()["custom"]])
async def test_json_rejects_unsupported_python_mode_values(unsupported: object) -> None:
    """Never coerce or silently switch codecs for an unsupported JSON graph."""
    with pytest.raises(ClickHouseEncodeError, match="result_payload_encode_failed"):
        await ResultCodec(JSONSerializer()).encode(success_result(return_value=unsupported))


@pytest.mark.asyncio
async def test_configured_codec_failure_never_falls_back() -> None:
    """Invoke the configured serializer once and return its safe failure."""
    serializer = FailingLoadSerializer(_HOSTILE_DETAIL)

    with pytest.raises(ClickHouseDecodeError) as raised:
        await ResultCodec(serializer).decode(b"configured-payload", None)

    assert serializer.load_calls == 1
    assert_safe_public_error(
        raised.value,
        operation="result_decode",
        reason="result_payload_decode_failed",
        forbidden=_HOSTILE_DETAIL,
    )


@pytest.mark.asyncio
async def test_taskiq_security_error_from_hostile_metadata_is_sanitized() -> None:
    """Translate Taskiq's payload-bearing SecurityError to a code-only error."""
    hostile = ExceptionRepr(
        exc_type="system",
        exc_message=(_HOSTILE_DETAIL,),
        exc_module="os",
    )
    payload = PickleSerializer().dumpb(
        {
            "is_err": True,
            "return_value": None,
            "execution_time": 0.5,
            "labels": {},
            "error": hostile,
        },
    )

    with pytest.raises(ClickHouseDataCorruptionError) as raised:
        await ResultCodec(PickleSerializer()).decode(payload, None)

    assert_safe_public_error(
        raised.value,
        operation="result_decode",
        reason="result_model_invalid",
        forbidden=_HOSTILE_DETAIL,
    )


@pytest.mark.asyncio
async def test_json_is_still_a_trusted_writer_boundary() -> None:
    """Show that valid JSON metadata can request known exception construction."""
    payload = JSONSerializer().dumpb(
        {
            "is_err": True,
            "return_value": None,
            "execution_time": 0.5,
            "labels": {},
            "error": {
                "exc_type": "ValueError",
                "exc_message": ["writer-controlled"],
                "exc_module": "builtins",
                "exc_cause": None,
                "exc_context": None,
                "exc_suppress_context": False,
            },
        },
    )

    decoded = await ResultCodec(JSONSerializer()).decode(payload, None)

    assert type(decoded.error) is ValueError
    assert str(decoded.error) == "writer-controlled"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field_name", "invalid_value"),
    [
        ("is_err", 1),
        ("execution_time", "0.5"),
        ("labels", []),
        ("error", "not-an-exception"),
    ],
)
async def test_decoded_scalar_shapes_are_strict(
    field_name: str,
    invalid_value: object,
) -> None:
    """Reject coercible persisted scalars at the Pydantic-2 boundary."""
    mapping = {
        "is_err": False,
        "return_value": None,
        "execution_time": 0.5,
        "labels": {},
        "error": None,
        field_name: invalid_value,
    }
    payload = PickleSerializer().dumpb(mapping)

    with pytest.raises(ClickHouseDataCorruptionError, match="result_model_invalid"):
        await ResultCodec(PickleSerializer()).decode(payload, None)


def test_parallel_public_error_writes_serialize_taskiq_model_conversion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Protect Taskiq's process-global exception cache across loops/backends."""
    gateway = CaptureGateway()
    probe = _ModelConversionProbe(prepare_exception)
    monkeypatch.setattr(taskiq_result_module, "prepare_exception", probe)

    with ThreadPoolExecutor(max_workers=1) as executor:
        writes = executor.submit(
            run_parallel_error_writes,
            gateway,
            count=_PARALLEL_WRITERS,
        )
        try:
            assert probe.entered.wait(timeout=_ASSERTION_TIMEOUT)
            assert not writes.done()
            assert probe.max_active == 1
        finally:
            probe.release.set()
        writes.result(timeout=_ASSERTION_TIMEOUT)

    assert probe.max_active == 1
    assert len(gateway.inserts) == _PARALLEL_WRITERS
