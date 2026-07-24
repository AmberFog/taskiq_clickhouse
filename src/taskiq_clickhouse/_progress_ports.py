"""Consumer-owned contracts for Taskiq progress orchestration."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol


if TYPE_CHECKING:
    from taskiq.depends.progress_tracker import TaskProgress


class StoredProgress(Protocol):
    """Minimal selected progress projection consumed by the use case."""

    @property
    def progress_payload(self) -> bytes:
        """Return the opaque serialized progress bytes."""
        ...


class ProgressCodec(Protocol):
    """Async Taskiq progress codec used by the public backend path."""

    async def encode(self, progress: TaskProgress[Any]) -> bytes:
        """Serialize one validated Python-mode progress mapping."""
        ...

    async def decode(self, progress_payload: bytes) -> TaskProgress[Any]:
        """Deserialize and validate one persisted progress mapping."""
        ...


class ProgressWriter(Protocol):
    """Persistence capability required by progress publication."""

    async def write_progress(
        self,
        task_id: str,
        progress_payload: bytes,
    ) -> object:
        """Persist one immutable progress generation."""
        ...


class ProgressReader(Protocol):
    """Persistence capability required by progress retrieval."""

    async def read_progress(self, task_id: str) -> StoredProgress | None:
        """Select the latest logically visible progress generation."""
        ...
