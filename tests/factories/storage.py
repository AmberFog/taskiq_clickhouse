"""Typed factories for immutable result and progress storage records."""

from datetime import UTC, datetime, timedelta

from factory.base import Factory
from factory.declarations import LazyAttribute, LazyAttributeSequence, Sequence

from taskiq_clickhouse._storage.progress_records import ProgressRecord
from taskiq_clickhouse._storage.result_records import RESULT_STATE, ResultRecord
from tests.factories import incidental


_BASE_TIME = datetime(2026, 7, 15, 12, tzinfo=UTC)
_VISIBLE_FOR = timedelta(hours=1)
_PURGE_AFTER = timedelta(hours=2)


class ResultRecordFactory(Factory[ResultRecord]):
    """Build valid result rows while keeping contract axes overridable."""

    class Meta:
        """Bind this factory to the exact result record type."""

        model = ResultRecord

    namespace = LazyAttributeSequence(  # type: ignore[no-untyped-call]
        lambda _record, sequence: incidental.identifier("namespace", sequence),
    )
    task_id = LazyAttributeSequence(  # type: ignore[no-untyped-call]
        lambda _record, sequence: incidental.identifier("task", sequence),
    )
    generation_at = Sequence(  # type: ignore[no-untyped-call]
        lambda sequence: _BASE_TIME + timedelta(microseconds=sequence),
    )
    generation_id = Sequence(incidental.uuid4)  # type: ignore[no-untyped-call]
    state = RESULT_STATE
    written_at = LazyAttribute(lambda record: record.generation_at)  # type: ignore[no-untyped-call]
    visible_until = LazyAttribute(  # type: ignore[no-untyped-call]
        lambda record: record.written_at + _VISIBLE_FOR,
    )
    purge_at = LazyAttribute(  # type: ignore[no-untyped-call]
        lambda record: record.written_at + _PURGE_AFTER,
    )
    result_payload = LazyAttributeSequence(  # type: ignore[no-untyped-call]
        lambda _record, sequence: incidental.payload("result", sequence),
    )
    log_payload = LazyAttributeSequence(  # type: ignore[no-untyped-call]
        lambda _record, sequence: incidental.payload("log", sequence),
    )


class ProgressRecordFactory(Factory[ProgressRecord]):
    """Build valid progress rows while keeping contract axes overridable."""

    class Meta:
        """Bind this factory to the exact progress record type."""

        model = ProgressRecord

    namespace = LazyAttributeSequence(  # type: ignore[no-untyped-call]
        lambda _record, sequence: incidental.identifier("namespace", sequence),
    )
    task_id = LazyAttributeSequence(  # type: ignore[no-untyped-call]
        lambda _record, sequence: incidental.identifier("task", sequence),
    )
    generation_at = Sequence(  # type: ignore[no-untyped-call]
        lambda sequence: _BASE_TIME + timedelta(microseconds=sequence),
    )
    generation_id = Sequence(incidental.uuid4)  # type: ignore[no-untyped-call]
    written_at = LazyAttribute(lambda record: record.generation_at)  # type: ignore[no-untyped-call]
    visible_until = LazyAttribute(  # type: ignore[no-untyped-call]
        lambda record: record.written_at + _VISIBLE_FOR,
    )
    purge_at = LazyAttribute(  # type: ignore[no-untyped-call]
        lambda record: record.written_at + _PURGE_AFTER,
    )
    progress_payload = LazyAttributeSequence(  # type: ignore[no-untyped-call]
        lambda _record, sequence: incidental.payload("progress", sequence),
    )
