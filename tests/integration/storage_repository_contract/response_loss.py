"""Typed response-loss cases for acknowledged storage writes."""

# ruff: noqa: S101

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Final

import pytest

from taskiq_clickhouse._clickhouse.request import InsertRequest
from taskiq_clickhouse._storage.queries import (
    PROGRESS_INSERT_COLUMN_NAMES,
    RESULT_INSERT_COLUMN_NAMES,
)
from taskiq_clickhouse._storage.repository import StorageRepository
from taskiq_clickhouse._storage.result_records import (
    RESULT_STATE,
    TOMBSTONE_STATE,
)


ResponseLossExercise = Callable[
    [StorageRepository, StorageRepository],
    Awaitable[None],
]

_TASK_ID: Final = "response-loss"


@dataclass(frozen=True, slots=True)
class ResponseLossCase:
    """Select one committed write response and prove its observable outcome."""

    column_names: tuple[str, ...]
    state: int | None
    exercise: ResponseLossExercise

    def matches(self, request: InsertRequest) -> bool:
        """Return whether this write is the case's one fault-injection target."""
        if len(request.rows) != 1 or tuple(request.column_names) != self.column_names:
            return False
        row = request.rows[0]
        if len(row) != len(self.column_names):
            return False
        if self.state is None:
            return True
        return row[self.column_names.index("state")] == self.state


async def _exercise_result_response_loss(
    repository: StorageRepository,
    ordinary_repository: StorageRepository,
) -> None:
    record = await repository.write_result(_TASK_ID, b"result", b"log")
    selected = await ordinary_repository.read_result_with_log(_TASK_ID)

    assert selected is not None
    assert selected.generation_id == record.generation_id
    assert (selected.result_payload, selected.log_payload) == (b"result", b"log")


async def _exercise_progress_response_loss(
    repository: StorageRepository,
    ordinary_repository: StorageRepository,
) -> None:
    record = await repository.write_progress(_TASK_ID, b"progress")
    selected = await ordinary_repository.read_progress(_TASK_ID)

    assert selected is not None
    assert selected.generation_id == record.generation_id
    assert selected.progress_payload == b"progress"


async def _exercise_tombstone_response_loss(
    repository: StorageRepository,
    ordinary_repository: StorageRepository,
) -> None:
    await ordinary_repository.write_result(_TASK_ID, b"result", b"log")
    selected = await ordinary_repository.read_result_no_log(_TASK_ID)
    assert selected is not None

    tombstone = await repository.write_tombstone(selected)

    assert tombstone.state == TOMBSTONE_STATE
    assert await ordinary_repository.read_result_no_log(_TASK_ID) is None


RESPONSE_LOSS_PARAMETERS: Final = (
    pytest.param(
        ResponseLossCase(
            column_names=RESULT_INSERT_COLUMN_NAMES,
            state=RESULT_STATE,
            exercise=_exercise_result_response_loss,
        ),
        id="result-after-commit",
    ),
    pytest.param(
        ResponseLossCase(
            column_names=PROGRESS_INSERT_COLUMN_NAMES,
            state=None,
            exercise=_exercise_progress_response_loss,
        ),
        id="progress-after-commit",
    ),
    pytest.param(
        ResponseLossCase(
            column_names=RESULT_INSERT_COLUMN_NAMES,
            state=TOMBSTONE_STATE,
            exercise=_exercise_tombstone_response_loss,
        ),
        id="tombstone-after-commit",
    ),
)
