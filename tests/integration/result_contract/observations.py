"""Observable public states returned by result-contract workflow actions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from typing import Any

    from taskiq.result import TaskiqResult


@dataclass(frozen=True, slots=True)
class BackendObservation:
    """One public readiness and result observation."""

    ready: bool
    task_result: TaskiqResult[Any]


@dataclass(frozen=True, slots=True)
class RewriteObservation:
    """Observable states around consumption and a fresh external write."""

    consumed: TaskiqResult[Any]
    ready_after_consume: bool
    missing_error: Exception | None
    fresh: BackendObservation


@dataclass(frozen=True, slots=True)
class NamespaceObservation:
    """Independent observations for two scoped backends."""

    first: BackendObservation
    second: BackendObservation


@dataclass(frozen=True, slots=True)
class ConcurrentConsumeObservation:
    """Two forced consumer outcomes and their final public state."""

    first: TaskiqResult[Any]
    second: TaskiqResult[Any]
    ready_after_consume: bool
    missing_error: Exception | None


@dataclass(frozen=True, slots=True)
class TargetedConsumeObservation:
    """Captured A and the latest state after writing and preserving B."""

    consumed: TaskiqResult[Any]
    latest: BackendObservation
