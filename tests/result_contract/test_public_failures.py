"""Exercise failure ordering through the public backend methods."""

import asyncio

import pytest
from taskiq.depends.progress_tracker import TaskProgress, TaskState
from taskiq.serializers.json_serializer import JSONSerializer

from taskiq_clickhouse._clickhouse.errors import AmbiguousClickHouseError, DefiniteClickHouseError
from taskiq_clickhouse._serialization import ResultCodec
from taskiq_clickhouse.exceptions import (
    ClickHouseBackendIOError,
    ClickHouseDecodeError,
    ClickHouseLifecycleError,
)
from tests.result_contract.assertions import (
    assert_production_traceback_excludes,
    assert_safe_public_error,
)
from tests.result_contract.backend_actions import (
    CaptureGateway,
    ScriptedGateway,
    allocation_row,
    readiness_row,
    result_row,
    running_backend,
    set_result_from_foreign_loop,
)
from tests.result_contract.models import success_result
from tests.result_contract.serializer_cases import CountingSerializer


_RAW_DETAIL = "password=PRIVATE_PASSWORD endpoint=private.internal payload=PRIVATE_PAYLOAD"
_PRIVATE_RESULT = "PRIVATE_RESULT_VALUE"
_PRIVATE_LOG = "PRIVATE_TASK_LOG"
_PRIVATE_TASK_ID = "private-task-id"


class _FatalPublicSignal(BaseException):
    """Represent one process-level signal at an exported data operation."""


@pytest.mark.asyncio
async def test_closed_public_operations_fail_before_codec_or_storage() -> None:
    """Reject every result operation before serializer or repository access."""
    gateway = CaptureGateway()
    serializer = CountingSerializer()
    async with running_backend(
        gateway=gateway,
        serializer=serializer,
        serializer_id="contract-counting-v1",
    ) as backend:
        await backend.shutdown()

        with pytest.raises(ClickHouseLifecycleError, match="not_ready"):
            await backend.set_result("closed", success_result())
        with pytest.raises(ClickHouseLifecycleError, match="not_ready"):
            await backend.is_result_ready("closed")
        with pytest.raises(ClickHouseLifecycleError, match="not_ready"):
            await backend.get_result("closed", with_logs=True)

    assert serializer.dump_calls == 0
    assert serializer.load_calls == 0
    assert gateway.query_calls == 0
    assert gateway.inserts == []


@pytest.mark.asyncio
async def test_foreign_loop_fails_before_codec_or_storage() -> None:
    """Reject same-process cross-loop access before touching loop-bound work."""
    gateway = CaptureGateway()
    serializer = CountingSerializer()
    async with running_backend(
        gateway=gateway,
        serializer=serializer,
        serializer_id="contract-counting-v1",
    ) as backend:
        with pytest.raises(ClickHouseLifecycleError, match="foreign_runtime"):
            await set_result_from_foreign_loop(backend, success_result())

    assert serializer.dump_calls == 0
    assert gateway.query_calls == 0
    assert gateway.inserts == []


@pytest.mark.asyncio
async def test_public_readiness_io_failure_never_becomes_false() -> None:
    """Preserve a classified transport failure instead of reporting not-ready."""
    gateway = ScriptedGateway(query_events=[AmbiguousClickHouseError(_RAW_DETAIL)])

    async with running_backend(gateway=gateway) as backend:
        with pytest.raises(ClickHouseBackendIOError) as raised:
            await backend.is_result_ready("io-failure")

    assert gateway.query_calls == 1
    assert_safe_public_error(
        raised.value,
        operation="result_readiness",
        reason="ambiguous_response",
        forbidden=_RAW_DETAIL,
    )


@pytest.mark.asyncio
async def test_public_decode_failure_does_not_consume() -> None:
    """Leave a corrupt latest result ready when decoding fails before tombstone."""
    gateway = ScriptedGateway(
        query_events=[result_row(b"{corrupt-result"), readiness_row()],
    )
    async with running_backend(gateway=gateway, keep_results=False) as backend:
        with pytest.raises(ClickHouseDecodeError) as raised:
            await backend.get_result("corrupt", with_logs=False)

        assert await backend.is_result_ready("corrupt")
    assert gateway.inserts == []
    assert_production_traceback_excludes(raised.value, b"{corrupt-result", "corrupt")


@pytest.mark.asyncio
async def test_ambiguous_tombstone_never_returns_payload() -> None:
    """Fail consume when neither the insert nor confirmation is acknowledged."""
    codec = ResultCodec(JSONSerializer())
    encoded = await codec.encode(success_result())
    gateway = ScriptedGateway(
        query_events=[
            result_row(encoded.result_payload),
            AmbiguousClickHouseError(_RAW_DETAIL),
        ],
        insert_events=[AmbiguousClickHouseError(_RAW_DETAIL)],
    )

    async with running_backend(gateway=gateway, keep_results=False) as backend:
        with pytest.raises(ClickHouseBackendIOError) as raised:
            await backend.get_result("ambiguous-consume", with_logs=False)

    assert len(gateway.inserts) == 1
    assert_safe_public_error(
        raised.value,
        operation="tombstone_write_confirm",
        reason="ambiguous_response",
        forbidden=_RAW_DETAIL,
    )
    assert_production_traceback_excludes(
        raised.value,
        encoded.result_payload,
        "ambiguous-consume",
    )


@pytest.mark.asyncio
async def test_public_result_write_failure_releases_payload_traceback_locals() -> None:
    """A classified insert failure retains neither the model nor its encoded values."""
    gateway = ScriptedGateway(
        query_events=[allocation_row()],
        insert_events=[DefiniteClickHouseError(_RAW_DETAIL)],
    )
    result = success_result(return_value={"value": _PRIVATE_RESULT}, log=_PRIVATE_LOG)

    async with running_backend(gateway=gateway) as backend:
        with pytest.raises(ClickHouseBackendIOError) as raised:
            await backend.set_result(_PRIVATE_TASK_ID, result)

    assert_production_traceback_excludes(
        raised.value,
        result,
        _PRIVATE_RESULT,
        _PRIVATE_LOG,
        _PRIVATE_TASK_ID,
    )


@pytest.mark.asyncio
async def test_public_result_write_cancellation_releases_payload_traceback_locals() -> None:
    """Preserve cancellation identity without retaining the interrupted write."""
    cancellation = asyncio.CancelledError("public-write-cancelled")
    cancellation.__cause__ = RuntimeError(_RAW_DETAIL)
    cancellation.__context__ = RuntimeError(_PRIVATE_LOG)
    gateway = ScriptedGateway(
        query_events=[allocation_row()],
        insert_events=[cancellation],
    )
    result = success_result(return_value={"value": _PRIVATE_RESULT}, log=_PRIVATE_LOG)

    async with running_backend(gateway=gateway) as backend:
        with pytest.raises(asyncio.CancelledError) as raised:
            await backend.set_result(_PRIVATE_TASK_ID, result)

    assert raised.value is cancellation
    assert raised.value.args == ("public-write-cancelled",)
    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None
    assert raised.value.__suppress_context__
    assert_production_traceback_excludes(
        raised.value,
        result,
        _PRIVATE_RESULT,
        _PRIVATE_LOG,
        _PRIVATE_TASK_ID,
    )


@pytest.mark.asyncio
async def test_public_result_write_fatal_signal_releases_payload_traceback_locals() -> None:
    """Preserve fatal identity without retaining the interrupted write."""
    fatal = _FatalPublicSignal()
    gateway = ScriptedGateway(
        query_events=[allocation_row()],
        insert_events=[fatal],
    )
    result = success_result(return_value={"value": _PRIVATE_RESULT}, log=_PRIVATE_LOG)

    async with running_backend(gateway=gateway) as backend:
        with pytest.raises(_FatalPublicSignal) as raised:
            await backend.set_result(_PRIVATE_TASK_ID, result)

    assert raised.value is fatal
    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None
    assert_production_traceback_excludes(
        raised.value,
        result,
        _PRIVATE_RESULT,
        _PRIVATE_LOG,
        _PRIVATE_TASK_ID,
    )


@pytest.mark.asyncio
async def test_public_progress_write_failure_releases_payload_traceback_locals() -> None:
    """Progress models and encoded meta do not survive a public package error."""
    gateway = ScriptedGateway(
        query_events=[allocation_row()],
        insert_events=[DefiniteClickHouseError(_RAW_DETAIL)],
    )
    progress = TaskProgress(
        state=TaskState.STARTED,
        meta={"private": _PRIVATE_RESULT},
    )

    async with running_backend(gateway=gateway) as backend:
        with pytest.raises(ClickHouseBackendIOError) as raised:
            await backend.set_progress(_PRIVATE_TASK_ID, progress)

    assert_production_traceback_excludes(
        raised.value,
        progress,
        _PRIVATE_RESULT,
        _PRIVATE_TASK_ID,
    )


@pytest.mark.asyncio
async def test_corrupt_log_fails_with_logs_then_no_log_consumes() -> None:
    """Ignore corrupt log bytes only on the physical no-log consume path."""
    codec = ResultCodec(JSONSerializer())
    expected = success_result(return_value={"value": 7})
    encoded = await codec.encode(expected)
    gateway = ScriptedGateway(
        query_events=[
            result_row(encoded.result_payload, log_payload=b"{corrupt-log"),
            readiness_row(),
            result_row(encoded.result_payload),
        ],
        insert_events=[None],
    )
    async with running_backend(gateway=gateway, keep_results=False) as backend:
        with pytest.raises(ClickHouseDecodeError):
            await backend.get_result("corrupt-log", with_logs=True)
        assert await backend.is_result_ready("corrupt-log")

        observed = await backend.get_result("corrupt-log", with_logs=False)

    assert observed.model_dump(exclude={"log"}) == expected.model_dump(exclude={"log"})
    assert observed.log is None
    assert len(gateway.inserts) == 1
    assert gateway.inserts[0].rows[0][4] == 1
