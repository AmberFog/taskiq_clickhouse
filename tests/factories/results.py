"""Factory for Taskiq result serialization contract tests."""

from typing import Any

from factory.base import Factory
from factory.declarations import Dict
from taskiq.result import TaskiqResult


class TaskiqResultFactory(Factory[TaskiqResult[Any]]):
    """Build a valid result with independent mutable fields."""

    class Meta:
        """Bind this factory to the exact Taskiq result model."""

        model = TaskiqResult

    is_err = False
    log = "task-log"
    return_value = None
    execution_time = 1.25
    labels = Dict(  # type: ignore[no-untyped-call]  # Factory Boy has no typed declaration API.
        {"queue": "tests"},
    )
    error = None
