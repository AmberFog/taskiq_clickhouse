"""Cross-event-loop admission for bounded synchronous executor submission."""

from __future__ import annotations

from asyncio import AbstractEventLoop, Future, get_running_loop
from collections import OrderedDict
from dataclasses import dataclass, field
from enum import Enum, auto
from threading import Lock
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from types import TracebackType
    from typing import Self


class _WaiterState(Enum):
    WAITING = auto()
    GRANTED = auto()
    ACQUIRED = auto()
    CANCELLED = auto()


@dataclass(slots=True, eq=False, repr=False)
class _AdmissionWaiter:
    ticket: int
    loop: AbstractEventLoop
    ready: Future[None]
    state: _WaiterState = _WaiterState.WAITING


@dataclass(slots=True, repr=False)
class SubmissionPermit:
    """Release one exact admission slot at most once."""

    _admission: SubmissionAdmission
    _released: bool = field(default=False, init=False)

    def __enter__(self) -> Self:
        """Enter the exact acquired-slot lifetime."""
        return self

    def __exit__(
        self,
        _exception_type: type[BaseException] | None,
        _exception: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        """Release the slot for every terminal caller outcome."""
        self.release()

    def release(self) -> None:
        """Return the owned slot and wake the oldest live waiter."""
        if self._released:
            return
        self._released = True
        self._admission.release()


class SubmissionAdmission:
    """Bound submitted work without binding waiters to one event loop."""

    __slots__ = (
        "_available",
        "_capacity",
        "_guard",
        "_next_ticket",
        "_waiters",
    )

    def __init__(self, capacity: int = 1) -> None:
        """Create a finite FIFO admission policy."""
        if capacity < 1:
            message = "submission capacity must be positive"
            raise ValueError(message)
        self._capacity = capacity
        self._available = capacity
        self._guard = Lock()
        self._next_ticket = 0
        self._waiters: OrderedDict[int, _AdmissionWaiter] = OrderedDict()

    async def acquire(self) -> SubmissionPermit:
        """Acquire one slot or wait on a future owned by the caller's loop."""
        loop = get_running_loop()
        ready = loop.create_future()
        with self._guard:
            if self._available > 0:
                self._available -= 1
                return SubmissionPermit(self)
            ticket = self._next_ticket
            self._next_ticket += 1
            waiter = _AdmissionWaiter(ticket, loop, ready)
            self._waiters[ticket] = waiter
        try:
            await ready
        except BaseException:  # Deregister before preserving any caller signal.
            self._cancel(waiter)
            raise
        with self._guard:
            waiter.state = _WaiterState.ACQUIRED
        return SubmissionPermit(self)

    def release(self) -> None:
        """Return one slot, transferring it across loops without blocking."""
        while True:
            waiter = self._grant_next()
            if waiter is None:
                return
            try:
                waiter.loop.call_soon_threadsafe(self._deliver, waiter)
            except RuntimeError:
                if self._revoke_closed_loop_grant(waiter):
                    continue
            return

    def _cancel(self, waiter: _AdmissionWaiter) -> None:
        release_grant = False
        with self._guard:
            if waiter.state is _WaiterState.WAITING:
                self._waiters.pop(waiter.ticket, None)
                waiter.state = _WaiterState.CANCELLED
            elif waiter.state is _WaiterState.GRANTED:
                waiter.state = _WaiterState.CANCELLED
                release_grant = True
        if release_grant:
            self.release()

    def _grant_next(self) -> _AdmissionWaiter | None:
        with self._guard:
            if not self._waiters:
                self._available = min(self._capacity, self._available + 1)
                return None
            _ticket, waiter = self._waiters.popitem(last=False)
            waiter.state = _WaiterState.GRANTED
            return waiter

    def _deliver(self, waiter: _AdmissionWaiter) -> None:
        release_grant = False
        with self._guard:
            if waiter.state is not _WaiterState.GRANTED:
                return
            if waiter.ready.done():
                waiter.state = _WaiterState.CANCELLED
                release_grant = True
            else:
                waiter.ready.set_result(None)
        if release_grant:
            self.release()

    def _revoke_closed_loop_grant(self, waiter: _AdmissionWaiter) -> bool:
        with self._guard:
            if waiter.state is not _WaiterState.GRANTED:
                return False
            waiter.state = _WaiterState.CANCELLED
            return True
