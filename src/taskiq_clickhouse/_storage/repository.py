"""Stable storage facade composed from result and progress repositories."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING
from uuid import uuid4

from taskiq_clickhouse._storage import generation, progress_records, queries, result_records
from taskiq_clickhouse._storage.acknowledged_writer import AcknowledgedWriter
from taskiq_clickhouse._storage.generation_allocator import GenerationAllocator
from taskiq_clickhouse._storage.layout import StorageLayout
from taskiq_clickhouse._storage.progress_repository import ProgressRepository
from taskiq_clickhouse._storage.result_repository import ResultRepository
from taskiq_clickhouse._storage_policy import StoragePolicy


if TYPE_CHECKING:
    from taskiq_clickhouse._clickhouse.contracts import ReadWriteGateway
    from taskiq_clickhouse._result_ports import StoredResult


@dataclass(frozen=True, slots=True, repr=False)
class StorageRepository:  # noqa: WPS214 - stable facade implements seven narrow backend operations.
    """Expose the unchanged backend store API over focused repositories."""

    gateway: ReadWriteGateway
    layout: StorageLayout
    policy: StoragePolicy
    uuid_factory: generation.UUIDFactory = uuid4
    _result_repository: ResultRepository = field(init=False, repr=False)
    _progress_repository: ProgressRepository = field(init=False, repr=False)

    def __post_init__(self) -> None:
        """Validate collaborators and compose focused immutable repositories."""
        _require_layout(self.layout)
        _require_policy(self.policy)
        _require_uuid_factory(self.uuid_factory)
        result_queries = queries.ResultQueries(self.layout.result_table)
        progress_queries = queries.ProgressQueries(self.layout.progress_table)
        allocator = GenerationAllocator(self.gateway, self.policy.retention, self.uuid_factory)
        writer = AcknowledgedWriter(
            self.gateway,
            self.layout,
            result_queries,
            progress_queries,
        )
        object.__setattr__(
            self,
            "_result_repository",
            ResultRepository(
                self.gateway,
                self.layout,
                self.policy,
                result_queries,
                allocator,
                writer,
            ),
        )
        object.__setattr__(
            self,
            "_progress_repository",
            ProgressRepository(
                self.gateway,
                self.policy,
                progress_queries,
                allocator,
                writer,
            ),
        )

    async def write_result(
        self,
        task_id: str,
        result_payload: bytes,
        log_payload: bytes,
    ) -> result_records.ResultRecord:
        """Persist one immutable result generation."""
        return await self._result_repository.write_result(task_id, result_payload, log_payload)

    async def write_progress(
        self,
        task_id: str,
        progress_payload: bytes,
    ) -> progress_records.ProgressRecord:
        """Persist one immutable progress generation."""
        return await self._progress_repository.write_progress(task_id, progress_payload)

    async def is_result_ready(self, task_id: str) -> bool:
        """Return whether the latest result state is logically visible."""
        return await self._result_repository.is_result_ready(task_id)

    async def read_result_no_log(self, task_id: str) -> result_records.ResultRead | None:
        """Select a visible result without fetching its log column."""
        return await self._result_repository.read_result_no_log(task_id)

    async def read_result_with_log(self, task_id: str) -> result_records.ResultRead | None:
        """Select a visible result and its log from one physical row."""
        return await self._result_repository.read_result_with_log(task_id)

    async def read_progress(self, task_id: str) -> progress_records.ProgressRead | None:
        """Select the latest logically visible progress generation."""
        return await self._progress_repository.read_progress(task_id)

    async def write_tombstone(
        self,
        selected: StoredResult,
    ) -> result_records.ResultRecord:
        """Acknowledge consumption of one exact selected result generation."""
        return await self._result_repository.write_tombstone(selected)


def _require_layout(candidate: object) -> None:
    if not isinstance(candidate, StorageLayout):
        msg = "layout must be a StorageLayout"
        raise TypeError(msg)


def _require_policy(candidate: object) -> None:
    if not isinstance(candidate, StoragePolicy):
        msg = "policy must be a StoragePolicy"
        raise TypeError(msg)


def _require_uuid_factory(candidate: object) -> None:
    if not callable(candidate):
        msg = "uuid_factory must be callable"
        raise TypeError(msg)
