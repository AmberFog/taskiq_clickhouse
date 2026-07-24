"""Isolated Taskiq progress model and serializer boundary."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from functools import partial
from typing import TYPE_CHECKING, Any, Final, TypeAlias

from taskiq.depends.progress_tracker import TaskProgress

from taskiq_clickhouse._executor_admission import SubmissionAdmission
from taskiq_clickhouse._mapping_snapshot import materialize_exact_mapping
from taskiq_clickhouse._serializer_boundary import (
    BOUNDARY_UNAVAILABLE_REASON,
    BoundaryFailure,
    SerializerBoundary,
    copy_exact_mapping,
    run_boundary,
)
from taskiq_clickhouse._taskiq_compat import PROGRESS_MODEL_FIELD_NAMES
from taskiq_clickhouse.exceptions import (
    ClickHouseDataCorruptionError,
    ClickHouseDecodeError,
    ClickHouseEncodeError,
)


if TYPE_CHECKING:
    from taskiq.abc.serializer import TaskiqSerializer


_PROGRESS_MODEL: Final = TaskProgress[Any]
_ENCODE_OPERATION: Final = "progress_encode"
_DECODE_OPERATION: Final = "progress_decode"
_MODEL_DUMP_FAILED: Final = "model_dump_failed"
_MODEL_DUMP_SHAPE: Final = "model_dump_shape"
_PAYLOAD_ENCODE_FAILED: Final = "progress_payload_encode_failed"
_PAYLOAD_NOT_BYTES: Final = "progress_payload_not_bytes"
_PAYLOAD_TYPE: Final = "progress_payload_type"
_PAYLOAD_DECODE_FAILED: Final = "progress_payload_decode_failed"
_PAYLOAD_SHAPE: Final = "progress_payload_shape"
_MODEL_INVALID: Final = "progress_model_invalid"

_ProgressSnapshot: TypeAlias = dict[str, object] | str


@dataclass(frozen=True, slots=True, repr=False)
class ProgressCodec:
    """Move synchronous Taskiq progress and serializer work off the event loop."""

    serializer: TaskiqSerializer = field(repr=False)
    serializer_admission: SubmissionAdmission = field(
        default_factory=SubmissionAdmission,
        repr=False,
    )

    async def encode(self, progress: TaskProgress[Any]) -> bytes:
        """Encode one exact Pydantic-2 Python-mode progress mapping."""
        captured = await run_boundary(partial(_capture_progress_snapshot, progress))
        if captured is BoundaryFailure.EXECUTOR_UNAVAILABLE:
            raise ClickHouseEncodeError(_ENCODE_OPERATION, BOUNDARY_UNAVAILABLE_REASON) from None
        if captured is BoundaryFailure.CALL_FAILED:
            raise ClickHouseEncodeError(_ENCODE_OPERATION, _MODEL_DUMP_FAILED) from None
        snapshot = captured.boundary_value
        if isinstance(snapshot, str):
            raise ClickHouseEncodeError(_ENCODE_OPERATION, snapshot) from None
        return await SerializerBoundary(
            self.serializer,
            self.serializer_admission,
        ).dump(
            snapshot,
            operation=_ENCODE_OPERATION,
            failed_reason=_PAYLOAD_ENCODE_FAILED,
            type_reason=_PAYLOAD_NOT_BYTES,
        )

    async def decode(self, progress_payload: bytes) -> TaskProgress[Any]:
        """Decode exact bytes into one strict Taskiq progress model."""
        if type(progress_payload) is not bytes:  # noqa: WPS516 - persisted type is exact.
            raise ClickHouseDataCorruptionError(_DECODE_OPERATION, _PAYLOAD_TYPE)
        decoded = await SerializerBoundary(
            self.serializer,
            self.serializer_admission,
        ).load(
            progress_payload,
            operation=_DECODE_OPERATION,
            failed_reason=_PAYLOAD_DECODE_FAILED,
        )
        normalized = await copy_exact_mapping(
            decoded,
            PROGRESS_MODEL_FIELD_NAMES,
            require_dict=False,
        )
        if normalized is BoundaryFailure.EXECUTOR_UNAVAILABLE:
            raise ClickHouseDecodeError(_DECODE_OPERATION, BOUNDARY_UNAVAILABLE_REASON) from None
        if normalized is BoundaryFailure.CALL_FAILED:
            raise ClickHouseDataCorruptionError(_DECODE_OPERATION, _PAYLOAD_SHAPE) from None
        return await _validate_progress(normalized.boundary_value)


def _capture_progress_snapshot(progress: TaskProgress[Any]) -> _ProgressSnapshot:
    try:
        dumped = progress.model_dump(mode="python", warnings="error")
    except Exception:  # noqa: BLE001 - Taskiq model implementations are untrusted.
        return _MODEL_DUMP_FAILED
    try:
        return materialize_exact_mapping(
            dumped,
            PROGRESS_MODEL_FIELD_NAMES,
            require_dict=True,
        )
    except asyncio.CancelledError:
        return _MODEL_DUMP_SHAPE
    except Exception:  # noqa: BLE001 - dumped mappings can execute hostile hooks.
        return _MODEL_DUMP_SHAPE


async def _validate_progress(candidate: dict[str, object]) -> TaskProgress[Any]:
    validated = await run_boundary(partial(_validate_progress_model, candidate))
    if validated is BoundaryFailure.EXECUTOR_UNAVAILABLE:
        raise ClickHouseDecodeError(_DECODE_OPERATION, BOUNDARY_UNAVAILABLE_REASON) from None
    if validated is BoundaryFailure.CALL_FAILED or not isinstance(
        validated.boundary_value,
        TaskProgress,
    ):
        raise ClickHouseDataCorruptionError(_DECODE_OPERATION, _MODEL_INVALID) from None
    return validated.boundary_value


def _validate_progress_model(candidate: dict[str, object]) -> object:
    return _PROGRESS_MODEL.model_validate(candidate, strict=True)
