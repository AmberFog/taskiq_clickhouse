"""Contract tests for shared typed test-data factories."""

from datetime import UTC, datetime, timedelta

from taskiq.result import TaskiqResult

from taskiq_clickhouse._schema.records import MetadataRecord, NamespaceContract
from taskiq_clickhouse._storage.progress_records import ProgressRecord
from taskiq_clickhouse._storage.result_records import ResultRecord
from tests.factories import incidental
from tests.factories.results import TaskiqResultFactory
from tests.factories.schema import MetadataRecordFactory, NamespaceContractFactory
from tests.factories.storage import ProgressRecordFactory, ResultRecordFactory


def test_incidental_factory_data_is_repeatable_without_shared_random_state() -> None:
    """Derive stable values from semantic coordinates without global seeding."""
    first = incidental.identifier("task", 7)

    assert incidental.identifier("task", 7) == first
    assert incidental.identifier("task", 8) != first
    assert incidental.payload("task", 7) == first.encode()


def test_schema_factories_build_exact_models_and_preserve_derived_contract() -> None:
    """Keep namespace evidence exact while allowing meaningful overrides."""
    namespace = NamespaceContractFactory.build(namespace="factory-contract")
    metadata = MetadataRecordFactory.from_namespace(namespace)

    assert type(namespace) is NamespaceContract
    assert type(metadata) is MetadataRecord
    assert metadata.scope == namespace.scope
    assert metadata.record_key == namespace.namespace
    assert metadata.payload == namespace.payload_bytes
    assert metadata.checksum == namespace.checksum


def test_storage_factories_keep_relative_deadlines_and_explicit_payloads() -> None:
    """Generate incidental identity while preserving scenario-owned values."""
    written_at = datetime(2026, 7, 24, 12, tzinfo=UTC)
    result = ResultRecordFactory.build(
        namespace="factory-contract",
        task_id="result",
        written_at=written_at,
        result_payload=b"exact-result",
        log_payload=b"exact-log",
    )
    progress = ProgressRecordFactory.build(
        namespace="factory-contract",
        task_id="progress",
        written_at=written_at,
        progress_payload=b"exact-progress",
    )

    assert type(result) is ResultRecord
    assert type(progress) is ProgressRecord
    assert result.visible_until == written_at + timedelta(hours=1)
    assert progress.purge_at == written_at + timedelta(hours=2)
    assert result.result_payload == b"exact-result"
    assert progress.progress_payload == b"exact-progress"


def test_taskiq_result_factory_does_not_share_mutable_labels() -> None:
    """Isolate mutable Taskiq labels between generated results."""
    first = TaskiqResultFactory.build()
    second = TaskiqResultFactory.build()

    first.labels["attempt"] = 1

    assert type(first) is TaskiqResult
    assert "attempt" not in second.labels
