"""Lazy process-owned thread pools for synchronous dependency boundaries."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import os
from threading import Lock
from typing import TYPE_CHECKING

from taskiq_clickhouse._executor_admission import (
    SubmissionAdmission,
    SubmissionPermit,
)


if TYPE_CHECKING:
    from collections.abc import Callable


_REGISTER_AT_FORK: Callable[..., None] | None = getattr(os, "register_at_fork", None)


class ProcessThreadPool:
    """Create one lazy executor per PID and discard inherited fork state."""

    __slots__ = (
        "_admission",
        "_executor",
        "_guard",
        "_max_workers",
        "_pid",
        "_pid_factory",
        "_submission_limit",
        "_thread_name_prefix",
    )

    def __init__(
        self,
        *,
        thread_name_prefix: str,
        max_workers: int | None = None,
        submission_limit: int = 32,
        pid_factory: Callable[[], int] = os.getpid,
    ) -> None:
        """Store construction policy without starting a worker thread."""
        self._thread_name_prefix = thread_name_prefix
        self._max_workers = max_workers
        self._submission_limit = submission_limit
        self._pid_factory = pid_factory
        self._pid = pid_factory()
        self._executor: ThreadPoolExecutor | None = None
        self._admission = SubmissionAdmission(submission_limit)
        self._guard = Lock()
        if _REGISTER_AT_FORK is not None:
            _REGISTER_AT_FORK(after_in_child=self._reset_after_fork)

    @property
    def executor(self) -> ThreadPoolExecutor:
        """Return the current process executor, creating it on first use."""
        current_pid = self._pid_factory()
        with self._guard:
            if current_pid != self._pid:
                self._reset_process_state(current_pid)
            if self._executor is None:
                self._executor = ThreadPoolExecutor(
                    max_workers=self._max_workers,
                    thread_name_prefix=self._thread_name_prefix,
                )
            return self._executor

    async def acquire(self) -> SubmissionPermit:
        """Wait for one process-local executor submission slot."""
        current_pid = self._pid_factory()
        with self._guard:
            if current_pid != self._pid:
                self._reset_process_state(current_pid)
            admission = self._admission
        return await admission.acquire()

    def _reset_after_fork(self) -> None:
        self._guard = Lock()
        self._reset_process_state(self._pid_factory())

    def _reset_process_state(self, current_pid: int) -> None:
        self._pid = current_pid
        self._executor = None
        self._admission = SubmissionAdmission(self._submission_limit)
