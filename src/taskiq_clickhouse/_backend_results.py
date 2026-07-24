"""Taskiq result operations over the READY storage and codec boundaries."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Final

from taskiq_clickhouse.exceptions import (
    ClickHouseConfigurationError,
    ClickHouseResultNotFoundError,
)


if TYPE_CHECKING:
    from typing import TypeVar

    from taskiq.result import TaskiqResult

    from taskiq_clickhouse._result_ports import (
        ResultCodec,
        ResultReadinessStore,
        ResultStore,
        ResultWriter,
        StoredResult,
    )

    _StoredResultT = TypeVar("_StoredResultT", bound=StoredResult)

_RESULT_READ: Final = "result_read"
_RESULT_READY: Final = "result_ready"
_RESULT_WRITE: Final = "result_write"
_INVALID_TASK_ID: Final = "invalid_task_id"
_INVALID_WITH_LOGS: Final = "invalid_with_logs"
_NOT_FOUND: Final = "not_found"


async def set_result(
    store: ResultWriter,
    codec: ResultCodec,
    task_id: object,
    task_result: TaskiqResult[Any],
) -> None:
    """Encode completely before writing one immutable result generation."""
    validated_task_id = _require_task_id(task_id, operation=_RESULT_WRITE)
    encoded = await codec.encode(task_result)
    await store.write_result(
        validated_task_id,
        encoded.result_payload,
        encoded.log_payload,
    )


async def is_result_ready(
    store: ResultReadinessStore,
    task_id: object,
) -> bool:
    """Check metadata-only readiness without reducing I/O failure to false."""
    validated_task_id = _require_task_id(task_id, operation=_RESULT_READY)
    return await store.is_result_ready(validated_task_id)


async def get_result(
    store: ResultStore[_StoredResultT],
    codec: ResultCodec,
    task_id: object,
    *,
    with_logs: object,
    keep_results: bool,
) -> TaskiqResult[Any]:
    """Decode a latest result and optionally acknowledge targeted consumption."""
    validated_task_id = _require_task_id(task_id, operation=_RESULT_READ)
    include_logs = _require_with_logs(with_logs)
    selected = (
        await store.read_result_with_log(validated_task_id)
        if include_logs
        else await store.read_result_no_log(validated_task_id)
    )
    if selected is None:
        raise ClickHouseResultNotFoundError(_RESULT_READ, _NOT_FOUND)
    decoded = await codec.decode(selected.result_payload, selected.log_payload)
    if not keep_results:
        await store.write_tombstone(selected)
    return decoded


def _require_task_id(candidate: object, *, operation: str) -> str:
    if type(candidate) is not str:  # noqa: WPS516 - reject coercible identifiers at the public boundary.
        raise ClickHouseConfigurationError(operation, _INVALID_TASK_ID)
    return candidate


def _require_with_logs(candidate: object) -> bool:
    if type(candidate) is not bool:  # noqa: WPS516 - Taskiq projection selection requires an exact boolean.
        raise ClickHouseConfigurationError(_RESULT_READ, _INVALID_WITH_LOGS)
    return candidate
