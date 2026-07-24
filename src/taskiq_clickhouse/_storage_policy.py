"""Validated immutable namespace and retention domain policies."""

from dataclasses import dataclass
import re
from typing import Final

from taskiq_clickhouse._datetime64 import (
    MAX_RETENTION_INTERVAL_US,
    add_microseconds,
    require_datetime64,
)


_NAMESPACE_PATTERN: Final = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")


@dataclass(frozen=True, slots=True, init=False)
class NamespaceKey:
    """One storage namespace accepted by every persistence boundary."""

    namespace: str

    def __init__(self, candidate: object) -> None:
        """Require the exact grammar persisted in namespace metadata."""
        if type(candidate) is not str:  # noqa: WPS516 - reject coercible subclasses at the trust boundary.
            msg = "namespace must match the storage contract"
            raise TypeError(msg)
        if _NAMESPACE_PATTERN.fullmatch(candidate) is None:
            msg = "namespace must match the storage contract"
            raise ValueError(msg)
        object.__setattr__(self, "namespace", candidate)


@dataclass(frozen=True, slots=True, init=False)
class RetentionPolicy:
    """Finite logical visibility and strictly later physical cleanup."""

    result_ttl_us: int
    purge_ttl_us: int

    def __init__(self, result_ttl_us: object, purge_ttl_us: object) -> None:
        """Require exact positive microseconds in deletion order."""
        result_ttl = _require_positive_microseconds(result_ttl_us, field="result_ttl_us")
        purge_ttl = _require_positive_microseconds(purge_ttl_us, field="purge_ttl_us")
        if result_ttl >= purge_ttl:
            msg = "result_ttl_us must be lower than purge_ttl_us"
            raise ValueError(msg)
        object.__setattr__(self, "result_ttl_us", result_ttl)
        object.__setattr__(self, "purge_ttl_us", purge_ttl)

    def require_feasible_at(self, observed_at: object) -> None:
        """Reject a purge deadline outside DateTime64 at server observation time."""
        timestamp = require_datetime64(
            observed_at,
            field="retention_observed_at",
        )
        add_microseconds(
            timestamp,
            self.purge_ttl_us,
            field="purge_at",
        )


@dataclass(frozen=True, slots=True)
class StoragePolicy:
    """One validated logical namespace and its finite retention contract."""

    namespace: NamespaceKey
    retention: RetentionPolicy

    def __post_init__(self) -> None:
        """Reject manually assembled policies containing raw primitives."""
        _require_policy_member(self.namespace, NamespaceKey, field="namespace")
        _require_policy_member(self.retention, RetentionPolicy, field="retention")


def _require_positive_microseconds(candidate: object, *, field: str) -> int:
    if type(candidate) is not int:  # noqa: WPS516 - booleans are not durations.
        msg = f"{field} must be an integer"
        raise TypeError(msg)
    if candidate <= 0:
        msg = f"{field} must be positive"
        raise ValueError(msg)
    if candidate > MAX_RETENTION_INTERVAL_US:
        msg = f"{field} must fit the DateTime64 retention range"
        raise ValueError(msg)
    return candidate


def _require_policy_member(candidate: object, member_type: type[object], *, field: str) -> None:
    if not isinstance(candidate, member_type):
        msg = f"{field} must be a {member_type.__name__}"
        raise TypeError(msg)
