"""Safe public exception hierarchy for the ClickHouse result backend."""

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
    "ClickHouseResultBackendError",
    "ClickHouseResultNotFoundError",
    "ClickHouseSchemaDriftError",
    "ClickHouseSchemaError",
    "ClickHouseSerializationError",
)

import re
from typing import Final

from taskiq.exceptions import ResultBackendError

from taskiq_clickhouse._schema_drift import SchemaDriftReport


_SAFE_CODE_PATTERN: Final = re.compile(r"[a-z][a-z0-9_]{0,63}\Z")


class ClickHouseResultBackendError(ResultBackendError):
    """Base error containing only package-owned safe reason codes."""

    __slots__ = ("operation", "reason")
    __template__ = "ClickHouse operation failed [{operation}:{reason}]"

    operation: str
    reason: str

    def __init__(self, operation: str, reason: str) -> None:
        """Initialize an error without accepting unsafe driver text."""
        safe_operation = _validate_safe_code(operation, field="operation")
        safe_reason = _validate_safe_code(reason, field="reason")
        super().__init__(operation=safe_operation, reason=safe_reason)


# Taskiq's Error uses dataclass_transform with keyword-only defaults.  Keep
# explicit subclass constructors so static analyzers retain this positional API.
class ClickHouseConfigurationError(ClickHouseResultBackendError):
    """Raised when backend configuration violates the public contract."""

    __slots__ = ()

    def __init__(self, operation: str, reason: str) -> None:
        """Initialize a configuration failure."""
        super().__init__(operation, reason)


class ClickHouseLifecycleError(ClickHouseResultBackendError):
    """Raised when an operation violates the backend lifecycle."""

    __slots__ = ()

    def __init__(self, operation: str, reason: str) -> None:
        """Initialize a lifecycle failure."""
        super().__init__(operation, reason)


class ClickHouseSchemaError(ClickHouseResultBackendError):
    """Base error for schema, migration and namespace failures."""

    __slots__ = ()

    def __init__(self, operation: str, reason: str) -> None:
        """Initialize a schema failure."""
        super().__init__(operation, reason)


class ClickHouseMigrationError(ClickHouseSchemaError):
    """Raised when migration history or execution is invalid."""

    __slots__ = ()

    def __init__(self, operation: str, reason: str) -> None:
        """Initialize a migration failure."""
        super().__init__(operation, reason)


class ClickHouseSchemaDriftError(ClickHouseSchemaError):
    """Raised when physical schema differs from its contract."""

    __slots__ = ()

    def __init__(self, operation: str, reason: str) -> None:
        """Initialize a physical drift failure."""
        super().__init__(operation, reason)


class _PhysicalSchemaDriftError(ClickHouseSchemaDriftError):
    """Carry value-free physical-drift coordinates through safe detachment."""

    __slots__ = ("report",)

    def __init__(self, report: SchemaDriftReport) -> None:
        """Bind one validated safe report to fixed public reason codes."""
        if type(report) is not SchemaDriftReport:  # noqa: WPS516 - raw/schema-specific payloads are forbidden here.
            msg = "physical drift error requires a safe report"
            raise TypeError(msg)
        self.report = report
        super().__init__("schema_validation", "physical_drift")


class ClickHouseNamespaceError(ClickHouseSchemaError):
    """Raised when a persisted namespace contract conflicts."""

    __slots__ = ()

    def __init__(self, operation: str, reason: str) -> None:
        """Initialize a namespace failure."""
        super().__init__(operation, reason)


class ClickHouseSerializationError(ClickHouseResultBackendError):
    """Base error for serialization and deserialization failures."""

    __slots__ = ()

    def __init__(self, operation: str, reason: str) -> None:
        """Initialize a serialization failure."""
        super().__init__(operation, reason)


class ClickHouseEncodeError(ClickHouseSerializationError):
    """Raised when a value cannot be encoded safely."""

    __slots__ = ()

    def __init__(self, operation: str, reason: str) -> None:
        """Initialize an encoding failure."""
        super().__init__(operation, reason)


class ClickHouseDecodeError(ClickHouseSerializationError):
    """Raised when persisted bytes cannot be decoded safely."""

    __slots__ = ()

    def __init__(self, operation: str, reason: str) -> None:
        """Initialize a decoding failure."""
        super().__init__(operation, reason)


class ClickHouseDataCorruptionError(ClickHouseResultBackendError):
    """Raised when a persisted row violates its data contract."""

    __slots__ = ()

    def __init__(self, operation: str, reason: str) -> None:
        """Initialize a data-corruption failure."""
        super().__init__(operation, reason)


class ClickHouseResultNotFoundError(ClickHouseResultBackendError):
    """Raised when a result is missing, expired or consumed."""

    __slots__ = ()

    def __init__(self, operation: str, reason: str) -> None:
        """Initialize a missing-result failure."""
        super().__init__(operation, reason)


class ClickHouseProgressError(ClickHouseResultBackendError):
    """Raised when progress data violates its contract."""

    __slots__ = ()

    def __init__(self, operation: str, reason: str) -> None:
        """Initialize a progress-data failure."""
        super().__init__(operation, reason)


class ClickHouseBackendIOError(ClickHouseResultBackendError):
    """Raised when ClickHouse I/O has a safe classified failure."""

    __slots__ = ()

    def __init__(self, operation: str, reason: str) -> None:
        """Initialize a classified ClickHouse I/O failure."""
        super().__init__(operation, reason)


_PUBLIC_ERROR_TYPES: tuple[type[ClickHouseResultBackendError], ...] = (
    ClickHouseConfigurationError,
    ClickHouseLifecycleError,
    ClickHouseMigrationError,
    ClickHouseSchemaDriftError,
    ClickHouseNamespaceError,
    ClickHouseSchemaError,
    ClickHouseEncodeError,
    ClickHouseDecodeError,
    ClickHouseSerializationError,
    ClickHouseDataCorruptionError,
    ClickHouseResultNotFoundError,
    ClickHouseProgressError,
    ClickHouseBackendIOError,
)


def rebuild_public_error(error: ClickHouseResultBackendError) -> ClickHouseResultBackendError:
    """Detach one error through its nearest stable public taxonomy class."""
    if type(error) is _PhysicalSchemaDriftError:  # noqa: WPS516 - structured payload belongs only to this exact package type.
        return _PhysicalSchemaDriftError(error.report)
    for error_type in _PUBLIC_ERROR_TYPES:
        if isinstance(error, error_type):
            return error_type(error.operation, error.reason)
    return ClickHouseResultBackendError(error.operation, error.reason)


def _validate_safe_code(code: object, *, field: str) -> str:
    if type(code) is not str:  # noqa: WPS516 - error formatting must not invoke str-subclass hooks.
        msg = f"{field} must be a safe package-owned code"
        raise TypeError(msg)
    if _SAFE_CODE_PATTERN.fullmatch(code) is None:
        msg = f"{field} must be a safe package-owned code"
        raise ValueError(msg)
    return code
