"""Typed factories for namespace contracts and metadata records."""

from datetime import UTC, datetime, timedelta

from factory.base import Factory
from factory.declarations import LazyAttribute, LazyAttributeSequence, Sequence

from taskiq_clickhouse._identifiers import Identifier, QualifiedTable
from taskiq_clickhouse._schema.canonical import canonical_json_bytes, sha256_hex
from taskiq_clickhouse._schema.migrations import MigrationDefinition
from taskiq_clickhouse._schema.records import MetadataRecord, NamespaceContract
from tests.factories import incidental


_DATABASE = Identifier("test_db")
_RESULT_TABLE = QualifiedTable(_DATABASE, Identifier("results"))
_PROGRESS_TABLE = QualifiedTable(_DATABASE, Identifier("progress"))
_RECORDED_AT = datetime(2026, 7, 15, 12, 30, 45, 123456, tzinfo=UTC)


class NamespaceContractFactory(Factory[NamespaceContract]):
    """Build one valid namespace contract with explicit override seams."""

    class Meta:
        """Bind this factory to the exact namespace contract type."""

        model = NamespaceContract

    namespace = LazyAttributeSequence(  # type: ignore[no-untyped-call]
        lambda _contract, sequence: incidental.identifier("namespace", sequence),
    )
    result_table = _RESULT_TABLE
    progress_table = _PROGRESS_TABLE
    serializer_id = "taskiq-json-v1"
    result_ttl_us = 1_000_000
    purge_ttl_us = 2_000_000


class MetadataRecordFactory(Factory[MetadataRecord]):
    """Build exact append-only metadata rows and derived schema evidence."""

    class Meta:
        """Bind this factory to the exact metadata record type."""

        model = MetadataRecord

    record_kind = "migration"
    scope = _RESULT_TABLE.canonical + "|" + _PROGRESS_TABLE.canonical
    record_key = "schema"
    version = 1
    name = "test-contract-v1"
    payload = LazyAttributeSequence(  # type: ignore[no-untyped-call]
        lambda _record, sequence: canonical_json_bytes(
            {"fixture": incidental.identifier("metadata", sequence)},
        ),
    )
    checksum = LazyAttribute(  # type: ignore[no-untyped-call]
        lambda record: sha256_hex(record.payload),
    )
    package_version = "0.1.0"
    recorded_at = Sequence(  # type: ignore[no-untyped-call]
        lambda sequence: _RECORDED_AT + timedelta(microseconds=sequence),
    )
    attempt_id = Sequence(incidental.uuid4)  # type: ignore[no-untyped-call]

    @classmethod
    def from_migration(cls, migration: MigrationDefinition) -> MetadataRecord:
        """Derive one persisted record from an immutable migration definition."""
        contract = NamespaceContractFactory.build(namespace="test-namespace")
        return cls.build(
            record_kind="migration",
            scope=contract.scope,
            record_key="schema",
            version=migration.version,
            name=migration.name,
            payload=migration.payload_bytes,
            checksum=migration.checksum,
        )

    @classmethod
    def from_namespace(cls, contract: NamespaceContract) -> MetadataRecord:
        """Derive one persisted namespace-contract record."""
        return cls.build(
            record_kind="namespace",
            scope=contract.scope,
            record_key=contract.namespace,
            version=1,
            name="namespace-contract-v1",
            payload=contract.payload_bytes,
            checksum=contract.checksum,
        )
