"""Verify bounded, fail-closed schema CLI diagnostics."""

from collections.abc import Callable
from typing import cast

import pytest

from taskiq_clickhouse import (
    _cli_drift_reporting as drift_reporting,
    _cli_reporting as cli_reporting,
)
from taskiq_clickhouse._identifiers import Identifier, QualifiedTable
from taskiq_clickhouse._schema_drift import SchemaDriftLocation, SchemaDriftReport
from taskiq_clickhouse.exceptions import ClickHouseMigrationError, _PhysicalSchemaDriftError


_GENERIC_FAILURE = "taskiq-clickhouse-schema: schema operation failed\n"
_SECRET = "password=reporting-secret dsn=https://private.internal"  # noqa: S105  # pragma: allowlist secret
_DETAIL_LIMIT = 50


class _HostileString(str):
    __slots__ = ()

    def __str__(self) -> str:
        """Fail if a diagnostic boundary evaluates an untrusted subclass."""
        raise RuntimeError(_SECRET)


class _PhysicalDriftSubclassError(_PhysicalSchemaDriftError):
    def __init__(self, report: SchemaDriftReport) -> None:
        """Preserve the internal error contract while changing its exact type."""
        super().__init__(report)


class _ReportSubclass(SchemaDriftReport):
    pass


class _LocationSubclass(SchemaDriftLocation):
    pass


class _TableSubclass(QualifiedTable):
    pass


class _IdentifierSubclass(Identifier):
    pass


class _TupleSubclass(tuple[SchemaDriftLocation, ...]):
    __slots__ = ()


def _table(database: str = "tasks", table: str = "results") -> QualifiedTable:
    return QualifiedTable(Identifier(database), Identifier(table))


def _location(
    path: str = "engine",
    *,
    table: QualifiedTable | None = None,
) -> SchemaDriftLocation:
    return SchemaDriftLocation(table or _table(), path)


def _drift_error(*locations: SchemaDriftLocation) -> _PhysicalSchemaDriftError:
    report = SchemaDriftReport(len(locations), locations)
    return _PhysicalSchemaDriftError(report)


def _mutated_table(
    *,
    database: object = Identifier("tasks"),
    table: object = Identifier("results"),
) -> QualifiedTable:
    candidate = _table()
    object.__setattr__(candidate, "database", database)
    object.__setattr__(candidate, "table", table)
    return candidate


def _mutated_identifier(value: object) -> Identifier:
    candidate = Identifier("safe")
    object.__setattr__(candidate, "value", value)
    return candidate


def test_operation_report_retains_only_valid_package_codes() -> None:
    """Known exact reason codes remain actionable; arbitrary errors do not."""
    public_error = ClickHouseMigrationError("migration_execute", "database_error")

    assert cli_reporting.render_operation_failure(public_error) == (
        "taskiq-clickhouse-schema: schema operation failed [migration_execute:database_error]\n"
    )
    assert cli_reporting.render_operation_failure(RuntimeError(_SECRET)) == _GENERIC_FAILURE


@pytest.mark.parametrize(
    ("operation", "reason"),
    [
        (_HostileString("migration_execute"), "database_error"),
        ("Migration_execute", "database_error"),
        ("migration_execute", _HostileString("database_error")),
        ("migration_execute", "database-error"),
    ],
)
def test_operation_report_rejects_unsafe_or_inexact_codes(
    operation: object,
    reason: object,
) -> None:
    """Malformed fields fail closed without invoking attacker-owned string hooks."""
    error = ClickHouseMigrationError("migration_execute", "database_error")
    object.__setattr__(error, "operation", operation)
    object.__setattr__(error, "reason", reason)

    report = cli_reporting.render_operation_failure(error)

    assert report == _GENERIC_FAILURE
    assert _SECRET not in report


def test_operation_report_fallback_survives_renderer_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A secondary reporting defect cannot replace or disclose the primary error."""

    def fail_renderer(error: Exception) -> tuple[str, ...]:
        del error
        raise RuntimeError(_SECRET)

    monkeypatch.setattr(cli_reporting, "render_drift_lines", fail_renderer)

    report = cli_reporting.render_operation_failure(
        ClickHouseMigrationError("migration_execute", "database_error"),
    )

    assert report == _GENERIC_FAILURE
    assert _SECRET not in report


@pytest.mark.parametrize(
    "error_factory",
    [
        lambda: RuntimeError(_SECRET),
        lambda: _PhysicalDriftSubclassError(SchemaDriftReport(1, (_location(),))),
    ],
)
def test_drift_report_requires_exact_internal_error(
    error_factory: Callable[[], Exception],
) -> None:
    """Public or subclassed errors cannot smuggle structured details to stderr."""
    assert drift_reporting.render_drift_lines(error_factory()) == ()


@pytest.mark.parametrize("malformed_report", [object(), _ReportSubclass(1, (_location(),))])
def test_drift_report_requires_exact_report_value(malformed_report: object) -> None:
    """Only the package's immutable report DTO crosses the renderer boundary."""
    error = _drift_error(_location())
    object.__setattr__(error, "report", malformed_report)

    assert drift_reporting.render_drift_lines(error) == ()


@pytest.mark.parametrize(
    "malformed_locations",
    [
        [_location()],
        _TupleSubclass((_location(),)),
        (),
    ],
)
def test_drift_report_rejects_mutable_inexact_or_empty_locations(
    malformed_locations: object,
) -> None:
    """Location containers must retain their validated exact immutable shape."""
    error = _drift_error(_location())
    object.__setattr__(error.report, "locations", malformed_locations)

    assert drift_reporting.render_drift_lines(error) == ()


def test_drift_report_filters_individual_hostile_coordinates() -> None:
    """One malformed coordinate cannot suppress adjacent safe coordinates."""
    safe_location = _location("columns[2].type")
    hostile_table = _location()
    object.__setattr__(hostile_table, "table", _TableSubclass(Identifier("tasks"), Identifier("other")))
    hostile_path = _location()
    object.__setattr__(hostile_path, "path", _HostileString("engine"))
    locations = cast(
        "tuple[object, ...]",
        (_LocationSubclass(_table(), "engine"), hostile_table, hostile_path, safe_location),
    )

    assert drift_reporting._safe_coordinates(locations) == (  # noqa: SLF001 - focused filter contract.
        ("tasks.results", "columns[2].type"),
    )


def test_drift_report_bounds_detail_count() -> None:
    """A large valid report exposes at most the fixed diagnostic budget."""
    locations = tuple(_location(f"columns[{index}].type") for index in range(_DETAIL_LIMIT + 1))

    lines = drift_reporting.render_drift_lines(_drift_error(*locations))

    assert lines[0] == (
        f"taskiq-clickhouse-schema: physical schema drift mismatches={_DETAIL_LIMIT + 1} reported={_DETAIL_LIMIT}"
    )
    assert len(lines) == _DETAIL_LIMIT + 1
    assert lines[-1].endswith(f"path=columns[{_DETAIL_LIMIT - 1}].type")
    assert all(f"columns[{_DETAIL_LIMIT}].type" not in line for line in lines)


def test_drift_report_can_report_zero_safe_coordinates() -> None:
    """The total mismatch count remains useful when every path is filtered."""
    assert drift_reporting.render_drift_lines(_drift_error(_location("unsupported.path"))) == (
        "taskiq-clickhouse-schema: physical schema drift mismatches=1 reported=0",
    )


@pytest.mark.parametrize(
    ("candidate", "expected"),
    [
        (object(), None),
        (_TableSubclass(Identifier("tasks"), Identifier("results")), None),
        (_table(), "tasks.results"),
        (_mutated_table(database=object()), None),
        (_mutated_table(table=object()), None),
        (_mutated_table(database=_IdentifierSubclass("tasks")), None),
        (_mutated_table(table=_IdentifierSubclass("results")), None),
        (_mutated_table(database=_mutated_identifier(object())), None),
        (_mutated_table(table=_mutated_identifier(object())), None),
        (_mutated_table(database=_mutated_identifier(_HostileString("tasks"))), None),
        (_mutated_table(database=_mutated_identifier("invalid-name")), None),
        (_mutated_table(table=_mutated_identifier("invalid-name")), None),
    ],
)
def test_table_filter_accepts_only_exact_valid_identifiers(
    candidate: object,
    expected: str | None,
) -> None:
    """Table rendering never evaluates inexact or invalid identifier values."""
    assert drift_reporting._safe_table(candidate) == expected  # noqa: SLF001 - focused filter contract.


@pytest.mark.parametrize(
    ("candidate", "expected"),
    [
        (object(), None),
        (_HostileString("engine"), None),
        ("engine", "engine"),
        ("settings.index_granularity", "settings"),
        ("settings.", None),
        ("settings." + "a" * 128, None),
        ("columns[0].type", "columns[0].type"),
        ("columns[9999].describe.compression_codec", "columns[9999].describe.compression_codec"),
        ("columns[2].secret", None),
        ("columns[01].type", None),
        ("columns[10000].type", None),
        ("auxiliary.unknown", None),
    ],
)
def test_path_filter_enforces_the_bounded_allowlist(
    candidate: object,
    expected: str | None,
) -> None:
    """Only known direct, setting, and bounded column coordinates are exposed."""
    assert drift_reporting._safe_path(candidate) == expected  # noqa: SLF001 - focused filter contract.
