"""Taskiq progress operations over explicit store and codec boundaries."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Final

from taskiq_clickhouse.exceptions import ClickHouseConfigurationError


if TYPE_CHECKING:
    from taskiq.depends.progress_tracker import TaskProgress

    from taskiq_clickhouse._progress_ports import ProgressCodec, ProgressReader, ProgressWriter


_PROGRESS_READ: Final = "progress_read"
_PROGRESS_WRITE: Final = "progress_write"
_INVALID_TASK_ID: Final = "invalid_task_id"


async def set_progress(
    store: ProgressWriter,
    codec: ProgressCodec,
    task_id: object,
    progress: TaskProgress[Any],
) -> None:
    """Encode completely before writing one immutable progress generation."""
    validated_task_id = _require_task_id(task_id, operation=_PROGRESS_WRITE)
    progress_payload = await codec.encode(progress)
    await store.write_progress(validated_task_id, progress_payload)


async def get_progress(
    store: ProgressReader,
    codec: ProgressCodec,
    task_id: object,
) -> TaskProgress[Any] | None:
    """Return latest visible progress without consuming its generation."""
    validated_task_id = _require_task_id(task_id, operation=_PROGRESS_READ)
    selected = await store.read_progress(validated_task_id)
    if selected is None:
        return None
    return await codec.decode(selected.progress_payload)


def _require_task_id(candidate: object, *, operation: str) -> str:
    if type(candidate) is not str:  # noqa: WPS516 - reject coercible identifiers at the public boundary.
        raise ClickHouseConfigurationError(operation, _INVALID_TASK_ID)
    return candidate
