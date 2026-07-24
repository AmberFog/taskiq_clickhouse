"""Behavior-shaped Taskiq boundary doubles."""

import asyncio
from typing import Any

from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.abc.result_backend import AsyncResultBackend
from taskiq.message import TaskiqMessage as ReceiverMessage
from taskiq.result import TaskiqResult


class PostSaveProbe(TaskiqMiddleware):
    """Record completion of Taskiq's save phase and optionally fail it."""

    def __init__(
        self,
        events: list[str],
        error: Exception | None = None,
    ) -> None:
        """Retain the scenario observations and optional failure."""
        super().__init__()
        self._events = events
        self._error = error

    def post_save(
        self,
        _message: ReceiverMessage,
        _result: TaskiqResult[Any],
    ) -> None:
        """Record the post-save boundary before returning or failing."""
        self._events.append("post_save")
        if self._error is not None:
            raise self._error


class CancellationResultBackend(AsyncResultBackend[Any]):
    """Cancel every result operation at the Taskiq wrapper boundary."""

    async def set_result(  # noqa: WPS615 - inherited Taskiq API.
        self,
        task_id: str,
        result: TaskiqResult[Any],  # noqa: WPS110 - inherited Taskiq API.
    ) -> None:
        """Cancel a write operation."""
        raise asyncio.CancelledError(task_id, result)

    async def is_result_ready(self, task_id: str) -> bool:
        """Cancel a readiness operation."""
        raise asyncio.CancelledError(task_id)

    async def get_result(  # noqa: WPS463, WPS615 - inherited Taskiq API.
        self,
        task_id: str,
        with_logs: bool = False,  # noqa: FBT001, FBT002 - inherited Taskiq signature.
    ) -> TaskiqResult[Any]:
        """Cancel a result read operation."""
        raise asyncio.CancelledError(task_id, with_logs)
