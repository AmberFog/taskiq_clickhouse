"""Storage records that establish ordering and visibility edge cases."""

from datetime import UTC, datetime, timedelta
from typing import Final

from taskiq_clickhouse._storage.progress_records import ProgressRecord
from taskiq_clickhouse._storage.result_records import (
    RESULT_STATE,
    TOMBSTONE_STATE,
    ResultRecord,
)
from tests.factories.storage import ProgressRecordFactory, ResultRecordFactory


_PARTITION_YEAR: Final = 2090


def visibility_result_records(
    namespace: str,
    now: datetime,
) -> tuple[ResultRecord, ...]:
    """Build result history whose latest rows are expired or tombstoned."""
    purge_at = now + timedelta(days=2)
    visible_at = now + timedelta(days=1)
    written_at = now - timedelta(minutes=10)
    expired_at = now - timedelta(minutes=1)
    return (
        ResultRecordFactory.build(
            namespace=namespace,
            task_id="expired-result",
            generation_at=now - timedelta(minutes=3),
            state=RESULT_STATE,
            written_at=written_at,
            visible_until=visible_at,
            purge_at=purge_at,
            result_payload=b"older-visible",
            log_payload=b"older-log",
        ),
        ResultRecordFactory.build(
            namespace=namespace,
            task_id="expired-result",
            generation_at=now - timedelta(minutes=2),
            state=RESULT_STATE,
            written_at=written_at,
            visible_until=expired_at,
            purge_at=purge_at,
            result_payload=b"newer-expired",
            log_payload=b"newer-log",
        ),
        ResultRecordFactory.build(
            namespace=namespace,
            task_id="tombstone-result",
            generation_at=now - timedelta(minutes=3),
            state=RESULT_STATE,
            written_at=written_at,
            visible_until=visible_at,
            purge_at=purge_at,
            result_payload=b"older-visible",
            log_payload=b"older-log",
        ),
        ResultRecordFactory.build(
            namespace=namespace,
            task_id="tombstone-result",
            generation_at=now - timedelta(minutes=2),
            state=TOMBSTONE_STATE,
            written_at=written_at,
            visible_until=visible_at,
            purge_at=purge_at,
            result_payload=b"",
            log_payload=b"",
        ),
        ResultRecordFactory.build(
            namespace=namespace,
            task_id="equality-result",
            generation_at=now - timedelta(minutes=1),
            state=RESULT_STATE,
            written_at=written_at,
            visible_until=visible_at,
            purge_at=purge_at,
            result_payload=b"boundary",
            log_payload=b"boundary-log",
        ),
    )


def visibility_progress_records(
    namespace: str,
    now: datetime,
) -> tuple[ProgressRecord, ...]:
    """Build progress history whose latest row is expired."""
    purge_at = now + timedelta(days=2)
    written_at = now - timedelta(minutes=10)
    return (
        ProgressRecordFactory.build(
            namespace=namespace,
            task_id="expired-progress",
            generation_at=now - timedelta(minutes=3),
            written_at=written_at,
            visible_until=now + timedelta(days=1),
            purge_at=purge_at,
            progress_payload=b"older-visible",
        ),
        ProgressRecordFactory.build(
            namespace=namespace,
            task_id="expired-progress",
            generation_at=now - timedelta(minutes=2),
            written_at=written_at,
            visible_until=now - timedelta(minutes=1),
            purge_at=purge_at,
            progress_payload=b"newer-expired",
        ),
    )


def future_result_record(
    namespace: str,
    written_at: datetime,
    generation_at: datetime,
) -> ResultRecord:
    """Build one future generation that allocation must exceed."""
    return ResultRecordFactory.build(
        namespace=namespace,
        task_id="same-point",
        generation_at=generation_at,
        state=RESULT_STATE,
        written_at=written_at,
        visible_until=written_at + timedelta(hours=2),
        purge_at=written_at + timedelta(hours=4),
        result_payload=b"future-seed",
        log_payload=b"",
    )


def partition_result_records(
    namespace: str,
    now: datetime,
) -> tuple[ResultRecord, ...]:
    """Build results whose generation order opposes purge partition order."""
    return (
        ResultRecordFactory.build(
            namespace=namespace,
            task_id="result-partitions",
            generation_at=now,
            state=RESULT_STATE,
            written_at=now,
            visible_until=now + timedelta(days=1),
            purge_at=datetime(_PARTITION_YEAR, 2, 1, tzinfo=UTC),
            result_payload=b"older-result",
            log_payload=b"older-log",
        ),
        ResultRecordFactory.build(
            namespace=namespace,
            task_id="result-partitions",
            generation_at=now + timedelta(microseconds=1),
            state=RESULT_STATE,
            written_at=now,
            visible_until=now + timedelta(days=1),
            purge_at=datetime(_PARTITION_YEAR, 1, 1, tzinfo=UTC),
            result_payload=b"newer-result",
            log_payload=b"newer-log",
        ),
    )


def partition_progress_records(
    namespace: str,
    now: datetime,
) -> tuple[ProgressRecord, ...]:
    """Build progress rows whose generation order opposes partition order."""
    return (
        ProgressRecordFactory.build(
            namespace=namespace,
            task_id="progress-partitions",
            generation_at=now,
            written_at=now,
            visible_until=now + timedelta(days=1),
            purge_at=datetime(_PARTITION_YEAR, 8, 1, tzinfo=UTC),
            progress_payload=b"older-progress",
        ),
        ProgressRecordFactory.build(
            namespace=namespace,
            task_id="progress-partitions",
            generation_at=now + timedelta(microseconds=1),
            written_at=now,
            visible_until=now + timedelta(days=1),
            purge_at=datetime(_PARTITION_YEAR, 7, 1, tzinfo=UTC),
            progress_payload=b"newer-progress",
        ),
    )
