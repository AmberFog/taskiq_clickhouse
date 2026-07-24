"""Test immutable schema, migration and metadata contracts."""

from dataclasses import FrozenInstanceError, replace
from datetime import UTC, datetime, timedelta, timezone
import math
from typing import TypeVar, cast
from uuid import UUID, uuid1

import pytest

from taskiq_clickhouse._identifiers import Identifier, QualifiedTable
from taskiq_clickhouse._schema.canonical import (
    canonical_json_bytes,
    decode_canonical_json,
    normalize_ddl,
    sha256_hex,
)
from taskiq_clickhouse._schema.contracts import (
    ColumnContract,
    SchemaContract,
    TableContract,
)
from taskiq_clickhouse._schema.migrations import (
    MigrationDefinition,
    MigrationStep,
    SchemaPlan,
)
from taskiq_clickhouse._schema.records import MetadataRecord, NamespaceContract
from taskiq_clickhouse._types import MigrationExecution, SchemaActor


UINT32_MAX = (1 << 32) - 1
SECOND_VERSION = 2
DATABASE = Identifier("analytics")
OTHER_DATABASE = Identifier("other")
RESULT_TABLE = QualifiedTable(DATABASE, Identifier("results"))
PROGRESS_TABLE = QualifiedTable(DATABASE, Identifier("progress"))
OTHER_TABLE = QualifiedTable(DATABASE, Identifier("other_table"))
RESULT_COLUMN = ColumnContract(Identifier("result_payload"), "String")
ADDITIVE_COLUMN = ColumnContract(
    Identifier("format_version"),
    "UInt8",
    default_kind="DEFAULT",
    default_expression="1",
)
RESULT_CONTRACT = TableContract(
    table=RESULT_TABLE,
    columns=(RESULT_COLUMN,),
    engine="MergeTree",
    partition_key="toYYYYMM(purge_at)",
    sorting_key="namespace, task_id",
    primary_key="namespace, task_id",
)
PROGRESS_CONTRACT = TableContract(
    table=PROGRESS_TABLE,
    columns=(ColumnContract(Identifier("progress_payload"), "String"),),
    engine="MergeTree",
    partition_key="toYYYYMM(purge_at)",
    sorting_key="namespace, task_id",
    primary_key="namespace, task_id",
)
BEFORE_SCHEMA = SchemaContract(absent_tables=(RESULT_TABLE,))
AFTER_SCHEMA = SchemaContract(tables=(RESULT_CONTRACT,))
BASE_STEP = MigrationStep(
    ddl="CREATE TABLE results (result_payload String) ENGINE = MergeTree ORDER BY tuple()",
    before=BEFORE_SCHEMA,
    after=AFTER_SCHEMA,
)
BASE_MIGRATION = MigrationDefinition(
    version=1,
    name="create-results",
    execution=MigrationExecution.AUTO,
    reentrant=True,
    concurrent_safe=True,
    steps=(BASE_STEP,),
)
BASE_NAMESPACE = NamespaceContract(
    namespace="default",
    result_table=RESULT_TABLE,
    progress_table=PROGRESS_TABLE,
    serializer_id="taskiq-json-v1",
    result_ttl_us=86_400_000_000,
    purge_ttl_us=604_800_000_000,
)
RECORDED_AT = datetime(2026, 7, 15, 12, 34, 56, 123456, tzinfo=UTC)
ATTEMPT_ID = UUID("00000000-0000-4000-8000-000000000001")
BASE_RECORD = MetadataRecord(
    record_kind="namespace",
    scope=BASE_NAMESPACE.scope,
    record_key=BASE_NAMESPACE.namespace,
    version=1,
    name="namespace-contract-v1",
    payload=BASE_NAMESPACE.payload_bytes,
    checksum=BASE_NAMESPACE.checksum,
    package_version="0.1.0",
    recorded_at=RECORDED_AT,
    attempt_id=ATTEMPT_ID,
)

ModelT = TypeVar("ModelT")


def unsafe_replace(instance: ModelT, **changes: object) -> ModelT:
    """Exercise runtime validation with deliberately mistyped field values."""
    return replace(instance, **changes)  # type: ignore[type-var]


def test_canonical_json_is_sorted_compact_utf8_and_round_trips() -> None:
    """Freeze exact UTF-8 bytes without ASCII escaping."""
    payload = canonical_json_bytes({"z": [True, None, 2], "ключ": "значение", "a": 1})

    assert payload == '{"a":1,"z":[true,null,2],"ключ":"значение"}'.encode()
    assert decode_canonical_json(payload) == {"a": 1, "z": [True, None, 2], "ключ": "значение"}


@pytest.mark.parametrize("value", [object(), math.nan, math.inf, -math.inf])
def test_canonical_json_rejects_unsupported_values(value: object) -> None:
    """Reject non-JSON values and non-finite numbers."""
    with pytest.raises(ValueError, match="encodable"):
        canonical_json_bytes({"value": value})


@pytest.mark.parametrize(
    "payload",
    [
        b"\xff",
        b"not-json",
        b'{"b":2, "a":1}',
        b'{"b":2,"a":1}',
        b'{"a":1,"a":1}',
        b'{"value":"\\u0430"}',
    ],
)
def test_decode_rejects_invalid_or_noncanonical_payload(payload: bytes) -> None:
    """Require byte-exact canonical input rather than semantic equivalence."""
    with pytest.raises(ValueError, match=r"valid UTF-8 JSON|canonical JSON form"):
        decode_canonical_json(payload)


def test_canonical_helpers_require_real_bytes() -> None:
    """Do not coerce bytearrays or text at checksum boundaries."""
    with pytest.raises(TypeError, match="payload must be bytes"):
        decode_canonical_json(cast("bytes", bytearray(b"{}")))
    with pytest.raises(TypeError, match="checksum payload must be bytes"):
        sha256_hex(cast("bytes", "{}"))


def test_sha256_uses_exact_payload_bytes() -> None:
    """Return stable lowercase SHA-256 hex."""
    expected = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"  # pragma: allowlist secret

    assert sha256_hex(b"abc") == expected


def test_normalize_ddl_changes_layout_only() -> None:
    """Normalize newlines and indentation while preserving expression spaces."""
    ddl = "\r\n    CREATE TABLE example\r\n    (value String DEFAULT 'two  spaces')   \r\n\r\n"

    assert normalize_ddl(ddl) == "CREATE TABLE example\n(value String DEFAULT 'two  spaces')"


@pytest.mark.parametrize(
    ("ddl", "error_type", "message"),
    [
        pytest.param(1, TypeError, "DDL must be a string", id="type"),
        pytest.param("SELECT '\x00'", ValueError, "must not contain NUL", id="nul"),
        pytest.param(" \r\n\t", ValueError, "must not be empty", id="empty"),
    ],
)
def test_normalize_ddl_rejects_invalid_input(ddl: object, error_type: type[Exception], message: str) -> None:
    """Reject values without one meaningful SQL statement."""
    with pytest.raises(error_type, match=message):
        normalize_ddl(cast("str", ddl))


def test_column_contract_normalizes_and_serializes_every_fact() -> None:
    """Retain exact normalized physical column facts."""
    column = ColumnContract(
        name=Identifier("value"),
        type_name="  String\r\n",
        default_kind=" DEFAULT ",
        default_expression=" 'two  spaces' ",
        compression_codec=" CODEC(ZSTD(1)) ",
        comment=" comment ",
    )

    assert column.canonical_data() == {
        "comment": "comment",
        "compression_codec": "CODEC(ZSTD(1))",
        "default_expression": "'two  spaces'",
        "default_kind": "DEFAULT",
        "name": "value",
        "ttl_expression": "",
        "type": "String",
    }


def test_column_contract_forbids_column_ttl() -> None:
    """Keep retention exclusively at table level for every managed column."""
    with pytest.raises(ValueError, match="must not declare column TTL"):
        replace(RESULT_COLUMN, ttl_expression="now()")


def test_column_contract_requires_identifier_and_nonempty_type() -> None:
    """Reject unvalidated names and empty physical types."""
    with pytest.raises(TypeError, match="column name must be a Identifier"):
        ColumnContract(cast("Identifier", object()), "String")
    with pytest.raises(ValueError, match="column type must not be empty"):
        ColumnContract(Identifier("value"), " ")


@pytest.mark.parametrize(
    ("field", "value", "error_type"),
    [
        pytest.param("type_name", object(), TypeError, id="type-name-type"),
        pytest.param("type_name", "String\x00", ValueError, id="type-name-nul"),
        pytest.param("default_kind", object(), TypeError, id="default-kind-type"),
        pytest.param("default_expression", "bad\x00", ValueError, id="default-expression-nul"),
        pytest.param("compression_codec", object(), TypeError, id="codec-type"),
        pytest.param("comment", "bad\x00", ValueError, id="comment-nul"),
        pytest.param("ttl_expression", object(), TypeError, id="ttl-type"),
    ],
)
def test_column_contract_rejects_invalid_catalog_text(field: str, value: object, error_type: type[Exception]) -> None:
    """Validate every catalog string without implicit coercion."""
    with pytest.raises(error_type):
        unsafe_replace(RESULT_COLUMN, **{field: value})


def test_table_contract_normalizes_settings_and_additive_allowlist() -> None:
    """Canonicalize semantically unordered compatibility declarations."""
    second_addition = ColumnContract(
        Identifier("another_default"),
        "String",
        default_kind="DEFAULT",
        default_expression="''",
    )
    table = replace(
        RESULT_CONTRACT,
        engine=" MergeTree ",
        critical_settings=(("z_setting", " 2 "), ("a_setting", " 1 ")),
        allowed_additive_columns=(ADDITIVE_COLUMN, second_addition),
        ttl_expression=" purge_at DELETE ",
    )

    assert table.critical_settings == (("a_setting", "1"), ("z_setting", "2"))
    assert tuple(column.name.value for column in table.allowed_additive_columns) == (
        "another_default",
        "format_version",
    )
    assert table.ttl_expression == "purge_at DELETE"
    assert table.canonical_data()["table"] == RESULT_TABLE.canonical
    assert table.canonical_data()["auxiliary_objects"] == {
        "constraints": [],
        "data_skipping_indices": [],
        "materialized_views": [],
        "projections": [],
    }


@pytest.mark.parametrize(
    ("field", "value", "error_type"),
    [
        pytest.param("table", object(), TypeError, id="table-type"),
        pytest.param("columns", [], TypeError, id="columns-mutable"),
        pytest.param("columns", (object(),), TypeError, id="column-type"),
        pytest.param("columns", (), ValueError, id="columns-empty"),
        pytest.param("engine", object(), TypeError, id="engine-type"),
        pytest.param("engine", "", ValueError, id="engine-empty"),
        pytest.param("sorting_key", " ", ValueError, id="sorting-key-empty"),
        pytest.param("primary_key", "\x00", ValueError, id="primary-key-nul"),
    ],
)
def test_table_contract_requires_typed_nonempty_core(
    field: str,
    value: object,
    error_type: type[Exception],
) -> None:
    """Require a qualified table, ordered columns, engine and keys."""
    with pytest.raises(error_type):
        unsafe_replace(RESULT_CONTRACT, **{field: value})


def test_table_contract_rejects_duplicate_columns() -> None:
    """Keep every physical column name unambiguous."""
    duplicate = ColumnContract(RESULT_COLUMN.name, "UInt8")

    with pytest.raises(ValueError, match=r"columns.*duplicates"):
        replace(RESULT_CONTRACT, columns=(RESULT_COLUMN, duplicate))


@pytest.mark.parametrize(
    "settings",
    [
        [],
        (["setting", "1"],),
        (("setting",),),
        (("setting", "1", "extra"),),
    ],
)
def test_table_contract_rejects_malformed_settings(settings: object) -> None:
    """Require immutable name/value setting pairs."""
    with pytest.raises(TypeError, match=r"critical_settings|name/value tuple"):
        unsafe_replace(RESULT_CONTRACT, critical_settings=settings)


@pytest.mark.parametrize(
    "settings",
    [
        (("bad-name", "1"),),
        ((1, "1"),),
        (("valid_name", ""),),
        (("valid_name", object()),),
        (("valid_name", "bad\x00"),),
        (("duplicate", "1"), ("duplicate", "2")),
    ],
)
def test_table_contract_rejects_invalid_settings(settings: object) -> None:
    """Reject unsafe, empty and duplicate package-critical settings."""
    with pytest.raises((TypeError, ValueError)):
        unsafe_replace(RESULT_CONTRACT, critical_settings=settings)


@pytest.mark.parametrize(
    "columns",
    [
        pytest.param([], id="mutable-sequence"),
        pytest.param((object(),), id="column-type"),
        pytest.param(
            (ADDITIVE_COLUMN, replace(ADDITIVE_COLUMN, type_name="UInt16")),
            id="duplicate-name",
        ),
        pytest.param((RESULT_COLUMN,), id="missing-default"),
        pytest.param((replace(ADDITIVE_COLUMN, default_kind=""),), id="default-kind-empty"),
        pytest.param(
            (replace(ADDITIVE_COLUMN, default_expression=""),),
            id="default-expression-empty",
        ),
        pytest.param((replace(ADDITIVE_COLUMN, default_kind="ALIAS"),), id="alias-kind"),
        pytest.param(
            (replace(ADDITIVE_COLUMN, default_kind="MATERIALIZED"),),
            id="materialized-kind",
        ),
    ],
)
def test_table_contract_requires_safe_additive_defaults(columns: object) -> None:
    """Permit only distinct additive columns with explicit defaults."""
    with pytest.raises((TypeError, ValueError)):
        unsafe_replace(RESULT_CONTRACT, allowed_additive_columns=columns)


def test_schema_contract_is_canonical_and_complete() -> None:
    """Sort present and absent tables by their qualified canonical names."""
    contract = SchemaContract(
        tables=(RESULT_CONTRACT, PROGRESS_CONTRACT),
        absent_tables=(OTHER_TABLE,),
    )

    assert tuple(item.table for item in contract.tables) == (PROGRESS_TABLE, RESULT_TABLE)
    assert contract.absent_tables == (OTHER_TABLE,)
    assert contract.canonical_data()["absent_tables"] == [OTHER_TABLE.canonical]


@pytest.mark.parametrize(
    ("field", "value", "error_type"),
    [
        pytest.param("tables", [], TypeError, id="tables-mutable"),
        pytest.param("tables", (object(),), TypeError, id="table-type"),
        pytest.param("absent_tables", [], TypeError, id="absent-tables-mutable"),
        pytest.param("absent_tables", (object(),), TypeError, id="absent-table-type"),
        pytest.param(
            "tables",
            (RESULT_CONTRACT, RESULT_CONTRACT),
            ValueError,
            id="duplicate-table",
        ),
        pytest.param(
            "absent_tables",
            (RESULT_TABLE, RESULT_TABLE),
            ValueError,
            id="duplicate-absent-table",
        ),
    ],
)
def test_schema_contract_rejects_malformed_or_duplicate_tables(
    field: str,
    value: object,
    error_type: type[Exception],
) -> None:
    """Reject mutable, mistyped and duplicate table collections."""
    with pytest.raises(error_type):
        unsafe_replace(BEFORE_SCHEMA, **{field: value})


def test_schema_contract_rejects_present_absent_overlap() -> None:
    """Reject a table declared both present and absent."""
    with pytest.raises(ValueError, match="present and absent"):
        SchemaContract(tables=(RESULT_CONTRACT,), absent_tables=(RESULT_TABLE,))


def test_migration_step_normalizes_and_covers_before_after() -> None:
    """Freeze one DDL plus complete structured contracts."""
    step = replace(BASE_STEP, ddl="\n  CREATE TABLE results  \r\n")

    assert step.ddl == "CREATE TABLE results"
    assert step.canonical_data() == {
        "after": AFTER_SCHEMA.canonical_data(),
        "before": BEFORE_SCHEMA.canonical_data(),
        "ddl": "CREATE TABLE results",
        "query_parameters": {},
    }


def test_migration_step_requires_contracts_and_a_change() -> None:
    """Reject unstructured or no-op migration steps."""
    with pytest.raises(TypeError, match="migration before state"):
        unsafe_replace(BASE_STEP, before=object())
    with pytest.raises(TypeError, match="migration after state"):
        unsafe_replace(BASE_STEP, after=object())
    with pytest.raises(ValueError, match="must change"):
        replace(BASE_STEP, before=AFTER_SCHEMA, after=AFTER_SCHEMA)


def test_migration_step_snapshots_and_covers_query_parameters() -> None:
    """Protect executable identifier bindings inside permanent history."""
    source = {"table": "results", "database": "analytics"}

    step = replace(BASE_STEP, query_parameters=source)
    source["table"] = "changed"

    assert step.query_parameters == {
        "database": "analytics",
        "table": "results",
    }
    assert step.canonical_data()["query_parameters"] == {
        "database": "analytics",
        "table": "results",
    }
    with pytest.raises(TypeError, match="does not support item assignment"):
        cast("dict[str, str]", step.query_parameters)["table"] = "changed"


@pytest.mark.parametrize(
    ("query_parameters", "error_type"),
    [
        pytest.param([], TypeError, id="not-mapping"),
        pytest.param({1: "results"}, TypeError, id="non-string-name"),
        pytest.param({"table": 1}, TypeError, id="non-string-value"),
        pytest.param({"": "results"}, ValueError, id="empty-name"),
        pytest.param({"table": ""}, ValueError, id="empty-value"),
        pytest.param({"table": "results\x00"}, ValueError, id="nul-value"),
    ],
)
def test_migration_step_rejects_invalid_query_parameters(
    query_parameters: object,
    error_type: type[Exception],
) -> None:
    """Reject bindings that cannot be canonical executable text."""
    with pytest.raises(error_type, match="migration query parameters"):
        unsafe_replace(BASE_STEP, query_parameters=query_parameters)


def test_migration_descriptor_covers_policy_steps_and_checksum() -> None:
    """Prevent execution-policy relabeling without a history conflict."""
    descriptor = decode_canonical_json(BASE_MIGRATION.payload_bytes)

    assert descriptor == BASE_MIGRATION.canonical_data()
    assert BASE_MIGRATION.payload_text == BASE_MIGRATION.payload_bytes.decode()
    assert BASE_MIGRATION.checksum == sha256_hex(BASE_MIGRATION.payload_bytes)
    assert BASE_MIGRATION.target == AFTER_SCHEMA
    controlled = replace(
        BASE_MIGRATION,
        execution=MigrationExecution.CONTROLLED,
        reentrant=False,
        concurrent_safe=False,
    )
    assert controlled.checksum != BASE_MIGRATION.checksum


def test_migration_descriptor_matches_frozen_golden_bytes() -> None:
    """Pin every permanent descriptor key before migration v1 is recorded."""
    expected = (
        b'{"concurrent_safe":true,"execution_class":"AUTO","name":"create-results",'
        b'"reentrant":true,"steps":[{"after":{"absent_tables":[],"tables":['
        b'{"allowed_additive_columns":[],"auxiliary_objects":{"constraints":[],'
        b'"data_skipping_indices":[],"materialized_views":[],"projections":[]},'
        b'"columns":[{"comment":"",'
        b'"compression_codec":"","default_expression":"","default_kind":"",'
        b'"name":"result_payload","ttl_expression":"","type":"String"}],'
        b'"critical_settings":[],"engine":"MergeTree",'
        b'"partition_key":"toYYYYMM(purge_at)","primary_key":"namespace, task_id",'
        b'"sampling_key":"","sorting_key":"namespace, task_id",'
        b'"table":"analytics.results","ttl_expression":""}]},'
        b'"before":{"absent_tables":["analytics.results"],"tables":[]},'
        b'"ddl":"CREATE TABLE results (result_payload String) ENGINE = MergeTree '
        b'ORDER BY tuple()","query_parameters":{}}],"version":1}'
    )
    expected_checksum = "e74bd9aca9184c81b2bdd7185225049eaa9d7aa22e9b893da0105ce336d9f2df"  # pragma: allowlist secret

    assert BASE_MIGRATION.payload_bytes == expected
    assert BASE_MIGRATION.checksum == expected_checksum


@pytest.mark.parametrize("version", [True, 0, -1, UINT32_MAX + 1, "1"])
def test_migration_rejects_invalid_uint32_version(version: object) -> None:
    """Require a real positive UInt32 version."""
    with pytest.raises((TypeError, ValueError)):
        unsafe_replace(BASE_MIGRATION, version=version)


@pytest.mark.parametrize(
    ("name", "error_type"),
    [
        pytest.param("", ValueError, id="empty"),
        pytest.param("UPPER", ValueError, id="uppercase"),
        pytest.param("has.dot", ValueError, id="punctuation"),
        pytest.param("a" * 129, ValueError, id="length"),
        pytest.param(1, TypeError, id="type"),
    ],
)
def test_migration_rejects_unstable_name(name: object, error_type: type[Exception]) -> None:
    """Require one stable lowercase migration name."""
    with pytest.raises(error_type, match="stable lowercase"):
        unsafe_replace(BASE_MIGRATION, name=name)


@pytest.mark.parametrize(
    ("field", "value", "error_type"),
    [
        pytest.param("execution", "AUTO", TypeError, id="execution-type"),
        pytest.param("reentrant", 1, TypeError, id="reentrant-type"),
        pytest.param("concurrent_safe", 1, TypeError, id="concurrent-safe-type"),
        pytest.param("steps", [], TypeError, id="steps-mutable"),
        pytest.param("steps", (object(),), TypeError, id="step-type"),
        pytest.param("steps", (), ValueError, id="steps-empty"),
    ],
)
def test_migration_rejects_invalid_policy_and_steps(
    field: str,
    value: object,
    error_type: type[Exception],
) -> None:
    """Require exact enums, booleans and immutable nonempty steps."""
    with pytest.raises(error_type):
        unsafe_replace(BASE_MIGRATION, **{field: value})


@pytest.mark.parametrize(
    ("reentrant", "concurrent_safe"),
    [(False, True), (True, False), (False, False)],
)
def test_auto_migration_requires_both_safety_flags(reentrant: object, concurrent_safe: object) -> None:
    """Never permit an unsafe definition on worker startup."""
    with pytest.raises(ValueError, match="reentrant and concurrent-safe"):
        unsafe_replace(BASE_MIGRATION, reentrant=reentrant, concurrent_safe=concurrent_safe)


def test_migration_steps_must_form_continuous_chain() -> None:
    """Detect partial plans whose adjacent contracts do not meet."""
    continuous_step = MigrationStep(
        ddl="CREATE TABLE progress (progress_payload String) ENGINE = MergeTree ORDER BY tuple()",
        before=AFTER_SCHEMA,
        after=SchemaContract(tables=(PROGRESS_CONTRACT, RESULT_CONTRACT)),
    )
    second_step = MigrationStep(
        ddl="CREATE TABLE progress (progress_payload String) ENGINE = MergeTree ORDER BY tuple()",
        before=SchemaContract(absent_tables=(PROGRESS_TABLE,)),
        after=SchemaContract(tables=(PROGRESS_CONTRACT,)),
    )

    assert replace(BASE_MIGRATION, steps=(BASE_STEP, continuous_step)).target == continuous_step.after
    with pytest.raises(ValueError, match="continuous chain"):
        replace(BASE_MIGRATION, steps=(BASE_STEP, second_step))


def test_schema_plan_requires_contiguous_unique_definitions() -> None:
    """Expose an exact target and reject gaps or reused names."""
    empty = SchemaPlan(())
    second_step = MigrationStep(
        ddl="CREATE TABLE progress (progress_payload String) ENGINE = MergeTree ORDER BY tuple()",
        before=AFTER_SCHEMA,
        after=SchemaContract(tables=(PROGRESS_CONTRACT, RESULT_CONTRACT)),
    )
    second = MigrationDefinition(
        version=2,
        name="create-progress",
        execution=MigrationExecution.AUTO,
        reentrant=True,
        concurrent_safe=True,
        steps=(second_step,),
    )

    assert empty.target_version == 0
    assert SchemaPlan((BASE_MIGRATION, second)).target_version == SECOND_VERSION
    with pytest.raises(ValueError, match="contiguous"):
        SchemaPlan((second,))
    disconnected_step = replace(second_step, before=SchemaContract(absent_tables=(OTHER_TABLE,)))
    with pytest.raises(ValueError, match=r"schema contracts.*continuous chain"):
        SchemaPlan((BASE_MIGRATION, replace(second, steps=(disconnected_step,))))
    with pytest.raises(ValueError, match="migration names"):
        SchemaPlan((BASE_MIGRATION, replace(second, name=BASE_MIGRATION.name)))


def test_schema_plan_requires_migration_tuple() -> None:
    """Reject mutable or incorrectly typed plans."""
    with pytest.raises(TypeError, match="tuple of MigrationDefinition"):
        SchemaPlan(cast("tuple[MigrationDefinition, ...]", []))
    with pytest.raises(TypeError, match="tuple of MigrationDefinition"):
        SchemaPlan(cast("tuple[MigrationDefinition, ...]", (object(),)))


def test_namespace_contract_matches_frozen_payload() -> None:
    """Keep namespace/scope out of the exact persisted payload body."""
    expected = (
        '{"payload_format":"taskiq-pydantic2-python-v1","progress_table":"analytics.progress",'
        '"purge_ttl_us":604800000000,"result_table":"analytics.results",'
        '"result_ttl_us":86400000000,"serializer_id":"taskiq-json-v1"}'
    )

    assert BASE_NAMESPACE.scope == "analytics.results|analytics.progress"
    assert BASE_NAMESPACE.payload_text == expected
    assert BASE_NAMESPACE.payload_bytes == expected.encode()
    assert BASE_NAMESPACE.checksum == sha256_hex(expected.encode())


@pytest.mark.parametrize(
    ("namespace", "error_type"),
    [
        pytest.param("", ValueError, id="empty"),
        pytest.param("-bad", ValueError, id="prefix"),
        pytest.param("has space", ValueError, id="space"),
        pytest.param("a" * 129, ValueError, id="length"),
        pytest.param("имя", ValueError, id="non-ascii"),
        pytest.param(1, TypeError, id="type"),
    ],
)
def test_namespace_contract_rejects_invalid_namespace(
    namespace: object,
    error_type: type[Exception],
) -> None:
    """Require the frozen ASCII namespace grammar."""
    with pytest.raises(error_type, match="namespace"):
        unsafe_replace(BASE_NAMESPACE, namespace=namespace)


def test_namespace_contract_requires_one_distinct_database_scope() -> None:
    """Keep result and progress names in one unambiguous scope."""
    with pytest.raises(TypeError, match="result table"):
        unsafe_replace(BASE_NAMESPACE, result_table=object())
    with pytest.raises(TypeError, match="progress table"):
        unsafe_replace(BASE_NAMESPACE, progress_table=object())
    with pytest.raises(ValueError, match="must differ"):
        replace(BASE_NAMESPACE, progress_table=RESULT_TABLE)
    with pytest.raises(ValueError, match="share one database"):
        replace(
            BASE_NAMESPACE,
            progress_table=QualifiedTable(OTHER_DATABASE, Identifier("progress")),
        )


@pytest.mark.parametrize(
    ("field", "value", "error_type"),
    [
        pytest.param("serializer_id", "", ValueError, id="empty-serializer"),
        pytest.param("serializer_id", "1bad", ValueError, id="serializer-prefix"),
        pytest.param("serializer_id", "has:colon", ValueError, id="serializer-character"),
        pytest.param("serializer_id", "a" * 65, ValueError, id="serializer-length"),
        pytest.param("serializer_id", 1, TypeError, id="serializer-type"),
        pytest.param("payload_format", "", ValueError, id="empty-format"),
        pytest.param("payload_format", "1bad", ValueError, id="format-prefix"),
    ],
)
def test_namespace_contract_rejects_invalid_storage_ids(
    field: str,
    value: object,
    error_type: type[Exception],
) -> None:
    """Require stable serializer and payload format identifiers."""
    with pytest.raises(error_type, match="storage identifier"):
        unsafe_replace(BASE_NAMESPACE, **{field: value})


@pytest.mark.parametrize(
    ("field", "value"),
    [
        pytest.param("result_ttl_us", True, id="result-bool"),
        pytest.param("result_ttl_us", 0, id="result-zero"),
        pytest.param("result_ttl_us", -1, id="result-negative"),
        pytest.param("result_ttl_us", 1.5, id="result-float"),
        pytest.param("purge_ttl_us", True, id="purge-bool"),
        pytest.param("purge_ttl_us", 0, id="purge-zero"),
    ],
)
def test_namespace_contract_rejects_invalid_ttl_types_and_values(field: str, value: object) -> None:
    """Require exact positive integer microseconds."""
    with pytest.raises((TypeError, ValueError)):
        unsafe_replace(BASE_NAMESPACE, **{field: value})


@pytest.mark.parametrize("purge_ttl_us", [BASE_NAMESPACE.result_ttl_us, BASE_NAMESPACE.result_ttl_us - 1])
def test_namespace_contract_requires_later_purge(purge_ttl_us: int) -> None:
    """Keep logical visibility strictly shorter than physical retention."""
    with pytest.raises(ValueError, match="lower than"):
        replace(BASE_NAMESPACE, purge_ttl_us=purge_ttl_us)


def test_metadata_record_is_exact_frozen_insert_row() -> None:
    """Return every physical column in the fixed insert order."""
    row = BASE_RECORD.as_row()

    assert row == (
        "namespace",
        BASE_NAMESPACE.scope,
        "default",
        1,
        "namespace-contract-v1",
        BASE_NAMESPACE.payload_bytes,
        BASE_NAMESPACE.checksum,
        "0.1.0",
        RECORDED_AT,
        ATTEMPT_ID,
    )
    with pytest.raises(FrozenInstanceError):
        BASE_RECORD.scope = "changed"  # type: ignore[misc]


@pytest.mark.parametrize("field", ["record_kind", "scope", "record_key", "name", "package_version"])
@pytest.mark.parametrize("value", ["", "bad\x00", 1])
def test_metadata_record_rejects_invalid_text_fields(field: str, value: object) -> None:
    """Do not coerce or retain empty/NUL metadata text."""
    with pytest.raises((TypeError, ValueError)):
        unsafe_replace(BASE_RECORD, **{field: value})


@pytest.mark.parametrize("version", [True, 0, -1, UINT32_MAX + 1, "1"])
def test_metadata_record_rejects_invalid_version(version: object) -> None:
    """Require the physical UInt32 range."""
    with pytest.raises((TypeError, ValueError)):
        unsafe_replace(BASE_RECORD, version=version)


@pytest.mark.parametrize(
    "payload",
    [b"not-json", b'{"b":2, "a":1}', bytearray(b"{}")],
)
def test_metadata_record_requires_canonical_payload(payload: object) -> None:
    """Reject invalid UTF-8/JSON and noncanonical bytes before insert."""
    with pytest.raises((TypeError, ValueError)):
        unsafe_replace(BASE_RECORD, payload=payload)


@pytest.mark.parametrize(
    ("checksum", "error_type"),
    [
        pytest.param("", ValueError, id="empty"),
        pytest.param("A" * 64, ValueError, id="uppercase"),
        pytest.param("0" * 63, ValueError, id="length"),
        pytest.param(1, TypeError, id="type"),
    ],
)
def test_metadata_record_rejects_invalid_checksum_shape(
    checksum: object,
    error_type: type[Exception],
) -> None:
    """Require exact lowercase SHA-256 hex."""
    with pytest.raises(error_type, match="lowercase SHA-256"):
        unsafe_replace(BASE_RECORD, checksum=checksum)


def test_metadata_record_rejects_checksum_mismatch() -> None:
    """Bind a metadata row to its exact canonical payload."""
    with pytest.raises(ValueError, match="does not match"):
        replace(BASE_RECORD, checksum="0" * 64)


@pytest.mark.parametrize(
    "recorded_at",
    [
        RECORDED_AT.replace(tzinfo=None),
        datetime(2026, 7, 15, tzinfo=timezone(timedelta(hours=1))),
        datetime(1899, 12, 31, 23, 59, 59, 999999, tzinfo=UTC),
        datetime(2300, 1, 1, tzinfo=UTC),
        "2026-07-15T00:00:00Z",
    ],
)
def test_metadata_record_requires_aware_utc_time(recorded_at: object) -> None:
    """Reject local, naive, non-datetime and out-of-ClickHouse timestamps."""
    with pytest.raises((TypeError, ValueError)):
        unsafe_replace(BASE_RECORD, recorded_at=recorded_at)


@pytest.mark.parametrize("attempt_id", [uuid1(), UUID(int=0), "uuid"])
def test_metadata_record_requires_random_uuid4(attempt_id: object) -> None:
    """Reject hostname/time-derived or malformed attempt identifiers."""
    with pytest.raises((TypeError, ValueError), match="UUIDv4"):
        unsafe_replace(BASE_RECORD, attempt_id=attempt_id)


def test_enum_values_are_frozen_protocol_tokens() -> None:
    """Keep policy tokens exact for checksums and runner decisions."""
    assert MigrationExecution.AUTO.value == "AUTO"
    assert MigrationExecution.CONTROLLED.value == "CONTROLLED"
    assert SchemaActor.WORKER.value == "WORKER"
    assert SchemaActor.MANAGER.value == "MANAGER"
