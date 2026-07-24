"""Exact Taskiq result model snapshot and strict validation policy."""

import asyncio
from typing import Any, Final, TypeAlias

from taskiq.result import TaskiqResult

from taskiq_clickhouse._mapping_snapshot import materialize_exact_mapping
from taskiq_clickhouse._taskiq_compat import RESULT_LOG_FIELD_NAME, RESULT_MODEL_FIELD_NAMES


MODEL_DUMP_FAILED: Final = "model_dump_failed"
MODEL_DUMP_SHAPE: Final = "model_dump_shape"
LOG_MODEL_INVALID: Final = "log_model_invalid"

_RESULT_MODEL: Final = TaskiqResult[Any]
LogValue: TypeAlias = str | None
ResultSnapshot: TypeAlias = tuple[dict[str, object], LogValue]


def capture_snapshot(taskiq_result: TaskiqResult[Any]) -> ResultSnapshot | str:
    """Dump and copy one exact six-field result model atomically."""
    try:
        dumped = taskiq_result.model_dump(
            mode="python",
            warnings="error",
        )
    except Exception:  # noqa: BLE001 - Taskiq model implementations are untrusted.
        return MODEL_DUMP_FAILED
    try:
        normalized = materialize_exact_mapping(
            dumped,
            RESULT_MODEL_FIELD_NAMES,
            require_dict=True,
        )
    except asyncio.CancelledError:
        return MODEL_DUMP_SHAPE
    except Exception:  # noqa: BLE001 - dumped mappings can execute hostile hooks.
        return MODEL_DUMP_SHAPE
    log = normalized.pop(RESULT_LOG_FIELD_NAME)
    if log is not None and type(log) is not str:  # noqa: WPS516 - persisted log type is exact.
        return LOG_MODEL_INVALID
    return normalized, log


def validate(candidate: dict[str, object]) -> object:
    """Apply strict Taskiq result validation inside the executor boundary."""
    return _RESULT_MODEL.model_validate(candidate, strict=True)


def is_result(candidate: object) -> bool:
    """Check the concrete generic Taskiq model returned by validation."""
    return isinstance(candidate, TaskiqResult)
