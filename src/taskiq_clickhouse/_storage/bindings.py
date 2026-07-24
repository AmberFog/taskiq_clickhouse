"""Typed parameter bindings for storage point and identity queries."""

from taskiq_clickhouse._storage.progress_records import ProgressRecord
from taskiq_clickhouse._storage.record_validation import require_text
from taskiq_clickhouse._storage.result_records import ResultRecord


def point_parameters(namespace: str, task_id: str) -> dict[str, object]:
    """Bind one namespace/task point without interpolating either value."""
    return {
        "namespace": require_text(namespace, field="namespace"),
        "task_id": require_text(task_id, field="task_id"),
    }


def result_confirmation_parameters(record: object) -> dict[str, object]:
    """Bind the complete logical identity of one frozen result row."""
    if not isinstance(record, ResultRecord):
        msg = "record must be a ResultRecord"
        raise TypeError(msg)
    return {
        **point_parameters(record.namespace, record.task_id),
        "generation_at": record.generation_at,
        "generation_id": record.generation_id,
        "state": record.state,
        "written_at": record.written_at,
        "visible_until": record.visible_until,
        "purge_at": record.purge_at,
    }


def progress_confirmation_parameters(record: object) -> dict[str, object]:
    """Bind the complete logical identity of one frozen progress row."""
    if not isinstance(record, ProgressRecord):
        msg = "record must be a ProgressRecord"
        raise TypeError(msg)
    return {
        **point_parameters(record.namespace, record.task_id),
        "generation_at": record.generation_at,
        "generation_id": record.generation_id,
    }
