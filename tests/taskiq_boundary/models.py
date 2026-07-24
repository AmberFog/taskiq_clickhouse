"""Test-only values crossing the Taskiq receiver boundary."""

from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel
from taskiq import AckableMessage, InMemoryBroker
from taskiq.receiver import Receiver
from taskiq.result import TaskiqResult

from taskiq_clickhouse.backend import ClickHouseResultBackend


class TypedReceiverResult(BaseModel):
    """Typed return value reconstructed by ``AsyncTaskiqTask``."""

    answer: int


class ReceiverTaskError(RuntimeError):
    """Module-visible custom task failure used for exact reconstruction."""


@dataclass(frozen=True, slots=True)
class ReceiverFailureCase:
    """Objects and observable events for one receiver save failure."""

    backend: ClickHouseResultBackend[Any]
    broker: InMemoryBroker
    receiver: Receiver
    message: AckableMessage
    events: list[str]


@dataclass(frozen=True, slots=True)
class TypedSuccessObservation:
    """Observable Taskiq state before and after one successful task."""

    ready_before_completion: bool
    waited: TaskiqResult[TypedReceiverResult]
    fetched: TaskiqResult[TypedReceiverResult]
