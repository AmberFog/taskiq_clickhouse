"""Isolated Taskiq result model and serializer boundary."""

from __future__ import annotations

from dataclasses import dataclass, field
from functools import partial
from typing import TYPE_CHECKING, Any, Final, cast

from taskiq_clickhouse._executor_admission import SubmissionAdmission
from taskiq_clickhouse._executor_pool import ProcessThreadPool
from taskiq_clickhouse._result_model import (
    MODEL_DUMP_FAILED,
    ResultSnapshot,
    capture_snapshot,
    is_result,
    validate as validate_result_model,
)
from taskiq_clickhouse._serializer_boundary import (
    BOUNDARY_UNAVAILABLE_REASON,
    BoundaryFailure,
    SerializerBoundary,
    copy_exact_mapping,
    run_boundary,
)
from taskiq_clickhouse._taskiq_compat import RESULT_PAYLOAD_FIELD_NAMES
from taskiq_clickhouse.exceptions import (
    ClickHouseDataCorruptionError,
    ClickHouseDecodeError,
    ClickHouseEncodeError,
)


if TYPE_CHECKING:
    from taskiq.abc.serializer import TaskiqSerializer
    from taskiq.result import TaskiqResult


# Taskiq's error serializer mutates one process-global recursion cache during
# model_dump.  One dedicated lane protects that cache without occupying every
# worker in asyncio's shared default executor behind a process-wide lock.
_TASKIQ_DUMP_EXECUTOR: Final = ProcessThreadPool(
    max_workers=1,
    submission_limit=1,
    thread_name_prefix="taskiq-clickhouse-model-dump",
)
_ENCODE_OPERATION: Final = "result_encode"
_DECODE_OPERATION: Final = "result_decode"
_RESULT_ENCODE_FAILED: Final = "result_payload_encode_failed"
_LOG_ENCODE_FAILED: Final = "log_payload_encode_failed"
_RESULT_NOT_BYTES: Final = "result_payload_not_bytes"
_LOG_NOT_BYTES: Final = "log_payload_not_bytes"
_RESULT_PAYLOAD_TYPE: Final = "result_payload_type"
_LOG_PAYLOAD_TYPE: Final = "log_payload_type"
_RESULT_DECODE_FAILED: Final = "result_payload_decode_failed"
_LOG_DECODE_FAILED: Final = "log_payload_decode_failed"
_RESULT_SHAPE: Final = "result_payload_shape"
_LOG_SHAPE: Final = "log_payload_shape"
_MODEL_INVALID: Final = "result_model_invalid"


@dataclass(frozen=True, slots=True, repr=False)
class EncodedResult:
    """Separate immutable bytes for one result mapping and optional log value."""

    result_payload: bytes
    log_payload: bytes


@dataclass(frozen=True, slots=True, repr=False)
class ResultCodec:
    """Move synchronous Taskiq model and serializer work off the event loop."""

    serializer: TaskiqSerializer = field(repr=False)
    serializer_admission: SubmissionAdmission = field(
        default_factory=SubmissionAdmission,
        repr=False,
    )

    async def encode(self, task_result: TaskiqResult[Any]) -> EncodedResult:
        """Extract one Python-mode model and encode result/log separately."""
        result_mapping, log = await _extract_result(task_result)
        result_payload = await _dump_value(
            self.serializer,
            self.serializer_admission,
            result_mapping,
            failed_reason=_RESULT_ENCODE_FAILED,
            type_reason=_RESULT_NOT_BYTES,
        )
        log_payload = await _dump_value(
            self.serializer,
            self.serializer_admission,
            log,
            failed_reason=_LOG_ENCODE_FAILED,
            type_reason=_LOG_NOT_BYTES,
        )
        return EncodedResult(result_payload, log_payload)

    async def decode(
        self,
        result_payload: bytes,
        log_payload: bytes | None,
    ) -> TaskiqResult[Any]:
        """Decode one exact result mapping and only an explicitly supplied log."""
        if type(result_payload) is not bytes:  # noqa: WPS516 - persisted payloads require exact bytes.
            raise ClickHouseDataCorruptionError(_DECODE_OPERATION, _RESULT_PAYLOAD_TYPE)
        decoded_result = await _load_value(
            self.serializer,
            self.serializer_admission,
            result_payload,
            failed_reason=_RESULT_DECODE_FAILED,
        )
        result_mapping = await _copy_result_mapping(decoded_result)
        log = await self._decode_log(log_payload)
        return await _validate_result({**result_mapping, "log": log})

    async def _decode_log(self, log_payload: bytes | None) -> str | None:
        if log_payload is None:
            return None
        if type(log_payload) is not bytes:  # noqa: WPS516 - persisted payloads require exact bytes.
            raise ClickHouseDataCorruptionError(_DECODE_OPERATION, _LOG_PAYLOAD_TYPE)
        decoded_log = await _load_value(
            self.serializer,
            self.serializer_admission,
            log_payload,
            failed_reason=_LOG_DECODE_FAILED,
        )
        if decoded_log is None or type(decoded_log) is str:  # noqa: WPS516 - decoded logs reject subclasses.
            return decoded_log
        raise ClickHouseDataCorruptionError(_DECODE_OPERATION, _LOG_SHAPE)


async def _extract_result(taskiq_result: TaskiqResult[Any]) -> ResultSnapshot:
    captured = await run_boundary(
        partial(capture_snapshot, taskiq_result),
        executor=_TASKIQ_DUMP_EXECUTOR,
    )
    if captured is BoundaryFailure.EXECUTOR_UNAVAILABLE:
        raise ClickHouseEncodeError(_ENCODE_OPERATION, BOUNDARY_UNAVAILABLE_REASON) from None
    if captured is BoundaryFailure.CALL_FAILED:
        raise ClickHouseEncodeError(
            _ENCODE_OPERATION,
            MODEL_DUMP_FAILED,
        ) from None
    snapshot = captured.boundary_value
    if isinstance(snapshot, str):
        raise ClickHouseEncodeError(_ENCODE_OPERATION, snapshot) from None
    return snapshot


async def _dump_value(
    serializer: TaskiqSerializer,
    admission: SubmissionAdmission,
    candidate: object,
    *,
    failed_reason: str,
    type_reason: str,
) -> bytes:
    return await SerializerBoundary(serializer, admission).dump(
        candidate,
        operation=_ENCODE_OPERATION,
        failed_reason=failed_reason,
        type_reason=type_reason,
    )


async def _load_value(
    serializer: TaskiqSerializer,
    admission: SubmissionAdmission,
    payload: bytes,
    *,
    failed_reason: str,
) -> object:
    return await SerializerBoundary(serializer, admission).load(
        payload,
        operation=_DECODE_OPERATION,
        failed_reason=failed_reason,
    )


async def _copy_result_mapping(candidate: object) -> dict[str, object]:
    normalized = await copy_exact_mapping(
        candidate,
        RESULT_PAYLOAD_FIELD_NAMES,
        require_dict=False,
    )
    if normalized is BoundaryFailure.EXECUTOR_UNAVAILABLE:
        raise ClickHouseDecodeError(_DECODE_OPERATION, BOUNDARY_UNAVAILABLE_REASON) from None
    if normalized is BoundaryFailure.CALL_FAILED:
        raise ClickHouseDataCorruptionError(_DECODE_OPERATION, _RESULT_SHAPE) from None
    return normalized.boundary_value


async def _validate_result(candidate: dict[str, object]) -> TaskiqResult[Any]:
    validated = await run_boundary(partial(validate_result_model, candidate))
    if validated is BoundaryFailure.EXECUTOR_UNAVAILABLE:
        raise ClickHouseDecodeError(_DECODE_OPERATION, BOUNDARY_UNAVAILABLE_REASON) from None
    if validated is BoundaryFailure.CALL_FAILED or not is_result(
        validated.boundary_value,
    ):
        raise ClickHouseDataCorruptionError(_DECODE_OPERATION, _MODEL_INVALID) from None
    return cast("TaskiqResult[Any]", validated.boundary_value)
