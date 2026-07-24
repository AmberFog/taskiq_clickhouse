"""Load packaged SQL and bind validated ClickHouse table identifiers."""

from collections.abc import Mapping
from pathlib import Path
from typing import Final, cast

from taskiq_clickhouse._identifiers import QualifiedTable


_SQL_ROOT: Final = Path(__file__).with_name("sql")
_RESERVED_TABLE_PARAMETERS: Final = frozenset(("database", "table"))


def load_sql(relative_path: str) -> str:
    """Read one non-empty UTF-8 SQL resource below the package SQL root."""
    resource_path = _relative_resource_path(relative_path)
    query = (_SQL_ROOT / resource_path).read_text(encoding="utf-8").strip()
    if not query:
        msg = "SQL resource must not be empty"
        raise ValueError(msg)
    return query


def bind_table(
    table: QualifiedTable,
    query_parameters: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """Copy values and bind one validated qualified table without interpolation."""
    validated_table = _qualified_table(table)
    copied = _copied_parameters(query_parameters)
    if _RESERVED_TABLE_PARAMETERS.intersection(copied):
        msg = "query parameters must not override database or table"
        raise ValueError(msg)
    return {
        "database": validated_table.database.value,
        "table": validated_table.table.value,
        **copied,
    }


def _relative_resource_path(candidate: object) -> Path:
    if not isinstance(candidate, str):
        msg = "SQL resource path must be a string"
        raise TypeError(msg)
    resource_path = Path(candidate)
    if not resource_path.parts or resource_path.is_absolute() or ".." in resource_path.parts:
        msg = "SQL resource path must be relative to the package SQL root"
        raise ValueError(msg)
    return resource_path


def _qualified_table(candidate: object) -> QualifiedTable:
    if not isinstance(candidate, QualifiedTable):
        msg = "table must be a QualifiedTable"
        raise TypeError(msg)
    return candidate


def _copied_parameters(candidate: object) -> dict[str, object]:
    if candidate is None:
        return {}
    if not isinstance(candidate, Mapping):
        msg = "query parameters must be a mapping"
        raise TypeError(msg)
    copied = dict(cast("Mapping[object, object]", candidate))
    if any(not isinstance(name, str) for name in copied):
        msg = "query parameter names must be strings"
        raise TypeError(msg)
    return cast("dict[str, object]", copied)
