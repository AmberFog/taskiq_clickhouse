"""Test the safe public exception hierarchy."""

import pytest
from taskiq.exceptions import ResultBackendError

from taskiq_clickhouse._identifiers import Identifier, QualifiedTable
from taskiq_clickhouse._schema._inspection_diff_types import SchemaDifference
from taskiq_clickhouse._schema_drift import (
    SchemaDriftLocation,
    SchemaDriftReport,
)
from taskiq_clickhouse.exceptions import (
    ClickHouseBackendIOError,
    ClickHouseConfigurationError,
    ClickHouseDataCorruptionError,
    ClickHouseDecodeError,
    ClickHouseEncodeError,
    ClickHouseLifecycleError,
    ClickHouseMigrationError,
    ClickHouseNamespaceError,
    ClickHouseProgressError,
    ClickHouseResultBackendError,
    ClickHouseResultNotFoundError,
    ClickHouseSchemaDriftError,
    ClickHouseSchemaError,
    ClickHouseSerializationError,
    _PhysicalSchemaDriftError,
    rebuild_public_error,
)


ERROR_TYPES = (
    ClickHouseBackendIOError,
    ClickHouseConfigurationError,
    ClickHouseDataCorruptionError,
    ClickHouseDecodeError,
    ClickHouseEncodeError,
    ClickHouseLifecycleError,
    ClickHouseMigrationError,
    ClickHouseNamespaceError,
    ClickHouseProgressError,
    ClickHouseResultBackendError,
    ClickHouseResultNotFoundError,
    ClickHouseSchemaDriftError,
    ClickHouseSchemaError,
    ClickHouseSerializationError,
)


class _StringSubclass(str):
    """Represent an unsafe string-shaped object with overridable hooks."""

    __slots__ = ()


@pytest.mark.parametrize("error_type", ERROR_TYPES)
def test_errors_have_safe_taskiq_base(error_type: type[ClickHouseResultBackendError]) -> None:
    """Expose only package-owned codes through string and repr output."""
    error = error_type("schema_validate", "physical_drift")

    assert isinstance(error, ResultBackendError)
    assert error.operation == "schema_validate"
    assert error.reason == "physical_drift"
    assert str(error) == "ClickHouse operation failed [schema_validate:physical_drift]"
    assert "schema_validate" in repr(error)
    assert "physical_drift" in repr(error)


@pytest.mark.parametrize(
    ("operation", "reason"),
    [
        pytest.param("", "valid", id="empty-operation"),
        pytest.param("UPPER", "valid", id="uppercase-operation"),
        pytest.param("has-hyphen", "valid", id="punctuated-operation"),
        pytest.param("a" * 65, "valid", id="long-operation"),
        pytest.param("valid", "", id="empty-reason"),
        pytest.param("valid", "bad value", id="spaced-reason"),
        pytest.param("valid", "a" * 65, id="long-reason"),
    ],
)
def test_errors_reject_unsafe_codes(operation: str, reason: str) -> None:
    """Never accept arbitrary driver text as a public message."""
    with pytest.raises(ValueError, match="safe package-owned code"):
        ClickHouseResultBackendError(operation, reason)


@pytest.mark.parametrize(
    ("operation", "reason"),
    [
        pytest.param(1, "valid", id="operation-type"),
        pytest.param("valid", object(), id="reason-type"),
        pytest.param(_StringSubclass("valid"), "valid", id="operation-subclass"),
        pytest.param("valid", _StringSubclass("valid"), id="reason-subclass"),
    ],
)
def test_errors_reject_non_string_codes(operation: object, reason: object) -> None:
    """Require real strings at the sanitization boundary."""
    with pytest.raises(TypeError, match="safe package-owned code"):
        ClickHouseResultBackendError(operation, reason)  # type: ignore[arg-type]


def test_exception_subtrees_are_precise() -> None:
    """Preserve schema and serialization subtype grouping."""
    assert issubclass(ClickHouseMigrationError, ClickHouseSchemaError)
    assert issubclass(ClickHouseSchemaDriftError, ClickHouseSchemaError)
    assert issubclass(ClickHouseNamespaceError, ClickHouseSchemaError)
    assert issubclass(ClickHouseEncodeError, ClickHouseSerializationError)
    assert issubclass(ClickHouseDecodeError, ClickHouseSerializationError)


@pytest.mark.parametrize("error_type", ERROR_TYPES)
def test_public_errors_rebuild_through_the_same_stable_type(
    error_type: type[ClickHouseResultBackendError],
) -> None:
    """Detach traceback state without relying on an internal subtype constructor."""
    original = error_type("schema_validate", "physical_drift")

    rebuilt = rebuild_public_error(original)

    assert rebuilt is not original
    assert type(rebuilt) is error_type
    assert rebuilt.operation == original.operation
    assert rebuilt.reason == original.reason


def test_physical_drift_rebuild_preserves_only_immutable_safe_report() -> None:
    """Detach an internal physical error without accepting a raw difference."""
    table = QualifiedTable(Identifier("tasks"), Identifier("results"))
    report = SchemaDriftReport(
        mismatch_count=1,
        locations=(SchemaDriftLocation(table, "columns[0].type"),),
    )
    original = _PhysicalSchemaDriftError(report)

    rebuilt = rebuild_public_error(original)

    assert type(rebuilt) is _PhysicalSchemaDriftError
    assert rebuilt is not original
    assert rebuilt.report is report
    assert "tasks" not in repr(rebuilt)
    assert "columns" not in str(rebuilt)
    with pytest.raises(TypeError, match="safe report"):
        _PhysicalSchemaDriftError(SchemaDifference(()))  # type: ignore[arg-type]
