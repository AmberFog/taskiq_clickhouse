"""Consumer-owned contracts for Taskiq result orchestration."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, TypeVar


if TYPE_CHECKING:
    from taskiq.result import TaskiqResult


class EncodedResult(Protocol):
    """Opaque split payload returned by the serializer boundary."""

    @property
    def result_payload(self) -> bytes:
        """Return the result-model bytes."""
        ...

    @property
    def log_payload(self) -> bytes:
        """Return the independently serialized log bytes."""
        ...


class StoredResult(Protocol):
    """Minimal selected result projection consumed by the use case."""

    @property
    def result_payload(self) -> bytes:
        """Return the opaque serialized result bytes."""
        ...

    @property
    def log_payload(self) -> bytes | None:
        """Return requested log bytes, or no log for the narrow projection."""
        ...


_StoredResultT = TypeVar("_StoredResultT", bound=StoredResult)


class ResultCodec(Protocol):
    """Async Taskiq result codec used by the public backend path."""

    async def encode(self, task_result: TaskiqResult[Any]) -> EncodedResult:
        """Serialize one result and its log into separate byte payloads."""
        ...

    async def decode(
        self,
        result_payload: bytes,
        log_payload: bytes | None,
    ) -> TaskiqResult[Any]:
        """Validate one requested result projection."""
        ...


class ResultWriter(Protocol):
    """Persistence capability required by result publication."""

    async def write_result(
        self,
        task_id: str,
        result_payload: bytes,
        log_payload: bytes,
    ) -> object:
        """Persist one immutable result generation."""
        ...


class ResultReadinessStore(Protocol):
    """Payload-free persistence capability required by readiness checks."""

    async def is_result_ready(self, task_id: str) -> bool:
        """Read the latest payload-free readiness state."""
        ...


class ResultStore(Protocol[_StoredResultT]):
    """Correlated selection and consumption capabilities for one store."""

    async def read_result_no_log(self, task_id: str) -> _StoredResultT | None:
        """Select the latest result without fetching its log."""
        ...

    async def read_result_with_log(self, task_id: str) -> _StoredResultT | None:
        """Select the latest result together with its log."""
        ...

    async def write_tombstone(self, selected: _StoredResultT) -> object:
        """Acknowledge consumption of one exact selected generation."""
        ...
