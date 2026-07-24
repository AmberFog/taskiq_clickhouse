"""Purpose-specific parameterized SQL for the frozen storage contract."""

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Final

from taskiq_clickhouse._identifiers import QualifiedTable
from taskiq_clickhouse._sql import bind_table, load_sql
from taskiq_clickhouse._storage.layout import (
    PROGRESS_COLUMN_NAMES,
    PROGRESS_COLUMN_TYPES,
    RESULT_COLUMN_NAMES,
    RESULT_COLUMN_TYPES,
)


RESULT_INSERT_COLUMN_NAMES: Final = RESULT_COLUMN_NAMES
RESULT_INSERT_COLUMN_TYPES: Final = RESULT_COLUMN_TYPES
PROGRESS_INSERT_COLUMN_NAMES: Final = PROGRESS_COLUMN_NAMES
PROGRESS_INSERT_COLUMN_TYPES: Final = PROGRESS_COLUMN_TYPES

_BYTES_FORMAT: Final = "bytes"
NO_LOG_COLUMN_FORMATS: Final = MappingProxyType({"result_payload": _BYTES_FORMAT})
WITH_LOG_COLUMN_FORMATS: Final = MappingProxyType(
    {
        "result_payload": _BYTES_FORMAT,
        "log_payload": _BYTES_FORMAT,
    },
)
PROGRESS_COLUMN_FORMATS: Final = MappingProxyType({"progress_payload": _BYTES_FORMAT})

_ALLOCATOR_QUERY: Final = load_sql("storage/allocate_generation.sql")
_RESULT_READINESS_QUERY: Final = load_sql("storage/result_readiness.sql")
_RESULT_NO_LOG_QUERY: Final = load_sql("storage/result_no_log.sql")
_RESULT_WITH_LOG_QUERY: Final = load_sql("storage/result_with_log.sql")
_RESULT_CONFIRMATION_QUERY: Final = load_sql("storage/result_confirmation.sql")
_PROGRESS_LATEST_QUERY: Final = load_sql("storage/progress_latest.sql")
_PROGRESS_CONFIRMATION_QUERY: Final = load_sql("storage/progress_confirmation.sql")


@dataclass(frozen=True, slots=True)
class ResultQueries:
    """Exact result/tombstone SQL and table bindings."""

    table: QualifiedTable

    def __post_init__(self) -> None:
        """Require one already-validated package-qualified table."""
        bind_table(self.table)

    def bind(
        self,
        query_parameters: Mapping[str, object] | None = None,
    ) -> dict[str, object]:
        """Add this query set's table identifiers to copied value parameters."""
        return bind_table(self.table, query_parameters)

    @property
    def allocator(self) -> str:
        """Return server-time and stored-maximum allocation SQL for results."""
        return _ALLOCATOR_QUERY

    @property
    def readiness(self) -> str:
        """Return the metadata-only latest-result readiness query."""
        return _RESULT_READINESS_QUERY

    @property
    def no_log(self) -> str:
        """Return latest-result SQL that cannot read the log column."""
        return _RESULT_NO_LOG_QUERY

    @property
    def with_log(self) -> str:
        """Return latest-result and log SQL selecting both from one row."""
        return _RESULT_WITH_LOG_QUERY

    @property
    def confirmation(self) -> str:
        """Return exact result/tombstone identity confirmation SQL."""
        return _RESULT_CONFIRMATION_QUERY


@dataclass(frozen=True, slots=True)
class ProgressQueries:
    """Exact progress SQL and table bindings."""

    table: QualifiedTable

    def __post_init__(self) -> None:
        """Require one already-validated package-qualified table."""
        bind_table(self.table)

    def bind(
        self,
        query_parameters: Mapping[str, object] | None = None,
    ) -> dict[str, object]:
        """Add this query set's table identifiers to copied value parameters."""
        return bind_table(self.table, query_parameters)

    @property
    def allocator(self) -> str:
        """Return server-time and stored-maximum allocation SQL."""
        return _ALLOCATOR_QUERY

    @property
    def latest(self) -> str:
        """Return the latest-progress query and its server observation time."""
        return _PROGRESS_LATEST_QUERY

    @property
    def confirmation(self) -> str:
        """Return exact progress identity confirmation SQL."""
        return _PROGRESS_CONFIRMATION_QUERY
