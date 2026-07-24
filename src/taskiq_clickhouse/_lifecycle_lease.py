"""Cross-thread claim around one event-loop-local lifecycle mutex."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
import os
from threading import Lock
from typing import TYPE_CHECKING, cast

from taskiq_clickhouse.exceptions import ClickHouseLifecycleError


if TYPE_CHECKING:
    from collections.abc import AsyncIterator


_FOREIGN_RUNTIME_REASON = "foreign_runtime"


@dataclass(frozen=True, slots=True)
class RuntimeIdentity:
    """One process and event-loop ownership identity."""

    pid: int
    loop: asyncio.AbstractEventLoop

    @classmethod
    def current(cls) -> RuntimeIdentity:
        """Capture the caller without touching a driver-owned object."""
        return cls(os.getpid(), asyncio.get_running_loop())

    def matches(self, candidate: RuntimeIdentity) -> bool:
        """Return whether another observation belongs to this runtime."""
        return self.pid == candidate.pid and self.loop is candidate.loop


class LifecycleLease:
    """Keep every active or queued lifecycle caller on one PID/event loop."""

    def __init__(self) -> None:
        self._guard = Lock()
        self._identity: RuntimeIdentity | None = None
        self._participants = 0
        self._async_lock = asyncio.Lock()

    @asynccontextmanager
    async def hold(self, operation: str) -> AsyncIterator[None]:
        """Claim one loop before awaiting its shared async mutex."""
        async_lock = self._claim(operation)
        try:
            async with async_lock:
                yield
        finally:
            self._release()

    def _claim(self, operation: str) -> asyncio.Lock:
        candidate = RuntimeIdentity.current()
        with self._guard:
            if self._participants == 0:
                self._identity = candidate
            elif not self._same_identity(candidate):
                raise ClickHouseLifecycleError(operation, _FOREIGN_RUNTIME_REASON)
            self._participants += 1
            return self._async_lock

    def _release(self) -> None:
        with self._guard:
            self._participants -= 1
            if self._participants == 0:
                self._identity = None
                self._async_lock = asyncio.Lock()

    def _same_identity(self, candidate: RuntimeIdentity) -> bool:
        return cast("RuntimeIdentity", self._identity).matches(candidate)
