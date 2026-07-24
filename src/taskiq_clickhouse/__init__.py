"""ClickHouse result backend integration for Taskiq."""

__all__ = (
    "ClickHouseBackendIOError",
    "ClickHouseConfigurationError",
    "ClickHouseDataCorruptionError",
    "ClickHouseDecodeError",
    "ClickHouseEncodeError",
    "ClickHouseLifecycleError",
    "ClickHouseMigrationError",
    "ClickHouseNamespaceError",
    "ClickHouseProgressError",
    "ClickHouseResultBackend",
    "ClickHouseResultBackendError",
    "ClickHouseResultNotFoundError",
    "ClickHouseSchemaDriftError",
    "ClickHouseSchemaError",
    "ClickHouseSerializationError",
    "ResultPersistenceReceiver",
    "SchemaMode",
)

from taskiq_clickhouse._types import SchemaMode
from taskiq_clickhouse.backend import ClickHouseResultBackend
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
)
from taskiq_clickhouse.receiver import ResultPersistenceReceiver
