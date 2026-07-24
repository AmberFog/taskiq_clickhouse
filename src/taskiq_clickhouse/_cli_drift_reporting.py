"""Project physical-schema drift into bounded, non-sensitive CLI details."""

import re
from typing import Final, TypeGuard, TypeVar

from taskiq_clickhouse._identifiers import Identifier, QualifiedTable
from taskiq_clickhouse._schema_drift import (
    SchemaDriftLocation,
    SchemaDriftReport,
)
from taskiq_clickhouse.exceptions import _PhysicalSchemaDriftError


_PROGRAM: Final = "taskiq-clickhouse-schema"
_IDENTIFIER_PATTERN: Final = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,126}\Z")
_SETTING_PATH_PATTERN: Final = re.compile(r"settings\.[A-Za-z_][A-Za-z0-9_]{0,126}\Z")
_COLUMN_PATH_PATTERN: Final = re.compile(r"columns\[(0|[1-9][0-9]{0,3})\]\.([a-z][a-z0-9_.]{0,63})\Z")
_DIRECT_PATHS: Final = (
    "auxiliary.constraints",
    "auxiliary.data_skipping_indices",
    "auxiliary.materialized_views",
    "auxiliary.projections",
    "columns.describe_count",
    "columns.describe_order",
    "columns.system_count",
    "columns.system_order",
    "columns.unexpected",
    "engine",
    "partition_key",
    "primary_key",
    "sampling_key",
    "sorting_key",
    "table",
    "ttl_expression",
)
_COLUMN_SUFFIXES: Final = (
    "compression_codec",
    "default_expression",
    "default_kind",
    "describe.comment",
    "describe.compression_codec",
    "describe.default_expression",
    "describe.default_kind",
    "describe.name",
    "describe.ttl_expression",
    "describe.type",
    "name",
    "position",
    "type",
)
_MAX_DETAILS: Final = 50
_DriftCoordinate = tuple[str, str]
_DriftReport = tuple[int, tuple[_DriftCoordinate, ...]]
_ExactT = TypeVar("_ExactT")


def render_drift_lines(error: Exception) -> tuple[str, ...]:
    """Render only package-known coordinates, never expected or actual values."""
    report = _drift_report(error)
    if report is None:
        return ()
    mismatch_count, coordinates = report
    return (
        f"{_PROGRAM}: physical schema drift mismatches={mismatch_count} reported={len(coordinates)}",
        *_detail_lines(coordinates),
    )


def _drift_report(error: Exception) -> _DriftReport | None:
    if not _has_exact_type(error, _PhysicalSchemaDriftError):
        return None
    report = error.report
    if not _has_exact_type(report, SchemaDriftReport):
        return None
    locations = report.locations
    if not _has_exact_type(locations, tuple) or not locations:
        return None
    return report.mismatch_count, _safe_coordinates(locations)


def _safe_coordinates(locations: tuple[object, ...]) -> tuple[_DriftCoordinate, ...]:
    coordinates: list[_DriftCoordinate] = []
    for location in locations[:_MAX_DETAILS]:
        if not _has_exact_type(location, SchemaDriftLocation):
            continue
        table = _safe_table(location.table)
        path = _safe_path(location.path)
        if table is not None and path is not None:
            coordinates.append((table, path))
    return tuple(coordinates)


def _detail_lines(coordinates: tuple[_DriftCoordinate, ...]) -> tuple[str, ...]:
    lines: list[str] = []
    for table, path in coordinates:
        lines.append(f"{_PROGRAM}: mismatch table={table} path={path}")
    return tuple(lines)


def _safe_table(candidate: object) -> str | None:
    if not _has_exact_type(candidate, QualifiedTable):
        return None
    database = candidate.database
    table = candidate.table
    if not _has_exact_type(database, Identifier) or not _has_exact_type(table, Identifier):
        return None
    names = (database.value, table.value)
    if not all(_has_exact_type(name, str) for name in names):
        return None
    if all(_IDENTIFIER_PATTERN.fullmatch(name) for name in names):
        return ".".join(names)
    return None


def _safe_path(candidate: object) -> str | None:
    if not _has_exact_type(candidate, str):
        return None
    if candidate in _DIRECT_PATHS:
        return candidate
    if _SETTING_PATH_PATTERN.fullmatch(candidate):
        return "settings"
    match = _COLUMN_PATH_PATTERN.fullmatch(candidate)
    if match:
        index, suffix = match.groups()
        if suffix in _COLUMN_SUFFIXES:
            return f"columns[{int(index)}].{suffix}"
    return None


def _has_exact_type(candidate: object, expected: type[_ExactT]) -> TypeGuard[_ExactT]:
    return type(candidate) is expected  # noqa: WPS516 - reject attacker-controlled diagnostic subclasses.
