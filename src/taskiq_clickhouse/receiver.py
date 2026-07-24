"""Taskiq receiver that commits broker ACK only after result persistence."""

__all__ = ("ResultPersistenceReceiver",)

import asyncio
from collections.abc import Callable
from concurrent.futures import Executor
from dataclasses import dataclass
import inspect
import logging
import math

from taskiq import AckableMessage
from taskiq.abc.broker import AsyncBroker
from taskiq.acks import AcknowledgeType
from taskiq.receiver import Receiver


_LOGGER = logging.getLogger("taskiq_clickhouse.receiver")
_WITHHELD_ACK_MESSAGE = (
    "Task processing did not reach confirmed broker settlement; "
    "acknowledgement was withheld or its outcome is ambiguous, "
    "and the worker consumer is stopping."
)
_STOPPED_REASON = "result_persistence_receiver_stopped"
_REUSED_RECEIVER = "ResultPersistenceReceiver can listen only once."
_INVALID_WAIT_TIMEOUT = "ResultPersistenceReceiver requires a finite positive wait_tasks_timeout."
_INVALID_MAX_ASYNC_TASKS = "ResultPersistenceReceiver requires max_async_tasks=1."
_INVALID_MAX_ASYNC_TASKS_JITTER = "ResultPersistenceReceiver requires max_async_tasks_jitter=0."
_INVALID_MAX_PREFETCH = "ResultPersistenceReceiver requires max_prefetch=1."
_INVALID_ACK_TYPE = "ResultPersistenceReceiver requires the WHEN_SAVED acknowledgement mode."
_ACKABLE_MESSAGE_REQUIRED = "ResultPersistenceReceiver requires broker messages with explicit acknowledgement."
_ACK_REQUEST_COUNT_INVALID = "Taskiq requested an unexpected number of acknowledgements."


class _ReceiverStoppedError(RuntimeError):
    """Terminate one Taskiq worker child with a stable, value-free reason."""


def _is_finite_positive_timeout(candidate: object) -> bool:
    """Validate a timeout without leaking numeric conversion failures."""
    if isinstance(candidate, bool):
        return False
    if not isinstance(candidate, (int, float)):
        return False
    try:
        return math.isfinite(candidate) and candidate > 0
    except OverflowError:
        return False


def _is_exact_integer(candidate: object, expected: int) -> bool:
    """Reject bool and equal numeric lookalikes at the CLI boundary."""
    return type(candidate) is int and candidate == expected  # noqa: WPS516


def _validate_single_delivery_limits(
    max_async_tasks: object,
    max_async_tasks_jitter: object,
    max_prefetch: object,
) -> None:
    """Require one deterministic in-flight delivery per receiver child."""
    if not _is_exact_integer(max_async_tasks, 1):
        raise ValueError(_INVALID_MAX_ASYNC_TASKS)
    if not _is_exact_integer(max_async_tasks_jitter, 0):
        raise ValueError(_INVALID_MAX_ASYNC_TASKS_JITTER)
    if not _is_exact_integer(max_prefetch, 1):
        raise ValueError(_INVALID_MAX_PREFETCH)


@dataclass(slots=True)
class _AcknowledgementGate:
    """Count Taskiq ACK requests without settling the broker delivery early."""

    requests: int = 0

    def request(self) -> None:
        """Record one acknowledgement request from Taskiq."""
        self.requests += 1


class ResultPersistenceReceiver(Receiver):
    """Defer every requested ACK until Taskiq's complete save phase succeeds."""

    def __init__(  # noqa: PLR0913, WPS211 - mirrors Taskiq's public Receiver extension point.
        self,
        broker: AsyncBroker,
        executor: Executor | None = None,
        validate_params: bool = True,  # noqa: FBT001, FBT002 - inherited API.
        max_async_tasks: int | None = None,
        max_async_tasks_jitter: int = 0,
        max_prefetch: int = 0,
        propagate_exceptions: bool = True,  # noqa: FBT001, FBT002 - inherited API.
        run_startup: bool = True,  # noqa: FBT001, FBT002 - inherited API.
        ack_type: AcknowledgeType | None = None,
        on_exit: Callable[[Receiver], None] | None = None,
        max_tasks_to_execute: int | None = None,
        wait_tasks_timeout: float | None = None,
    ) -> None:
        """Validate the bounded single-delivery worker contract."""
        if wait_tasks_timeout is None or not _is_finite_positive_timeout(wait_tasks_timeout):
            raise ValueError(_INVALID_WAIT_TIMEOUT)
        _validate_single_delivery_limits(max_async_tasks, max_async_tasks_jitter, max_prefetch)
        effective_ack_type = AcknowledgeType.WHEN_SAVED if ack_type is None else ack_type
        if effective_ack_type is not AcknowledgeType.WHEN_SAVED:
            raise ValueError(_INVALID_ACK_TYPE)
        super().__init__(
            broker=broker,
            executor=executor,
            validate_params=validate_params,
            max_async_tasks=max_async_tasks,
            max_async_tasks_jitter=max_async_tasks_jitter,
            max_prefetch=max_prefetch,
            propagate_exceptions=propagate_exceptions,
            run_startup=run_startup,
            ack_type=effective_ack_type,
            on_exit=on_exit,
            max_tasks_to_execute=max_tasks_to_execute,
            wait_tasks_timeout=wait_tasks_timeout,
        )
        self._acknowledgement_timeout = float(wait_tasks_timeout)
        self._listen_started = False
        self._failure_event: asyncio.Event | None = None
        self._finish_event: asyncio.Event | None = None

    async def listen(self, finish_event: asyncio.Event) -> None:
        """Run Taskiq's listener and fail the worker child after a withheld ACK."""
        if self._failure_event is not None:
            message = "ResultPersistenceReceiver cannot listen concurrently."
            raise RuntimeError(message)
        if self._listen_started:
            raise RuntimeError(_REUSED_RECEIVER)
        self._listen_started = True
        failure_event = asyncio.Event()
        self._failure_event = failure_event
        self._finish_event = finish_event
        try:  # noqa: WPS501 - listener state must be released on every exit.
            await super().listen(finish_event)
        finally:
            self._failure_event = None
            self._finish_event = None
        if failure_event.is_set():
            raise _ReceiverStoppedError(_STOPPED_REASON)

    async def callback(
        self,
        message: bytes | AckableMessage,
        raise_err: bool = False,  # noqa: FBT001, FBT002 - inherited Taskiq API.
    ) -> None:
        """Execute one delivery and commit its ACK only after confirmed saving."""
        if self._failure_event is not None and self._failure_event.is_set():
            if raise_err:
                raise _ReceiverStoppedError(_STOPPED_REASON)
            return
        if not isinstance(message, AckableMessage):
            self._stop_consumer()
            raise TypeError(_ACKABLE_MESSAGE_REQUIRED)
        ackable_message = message
        gate = _AcknowledgementGate()
        gated_message = ackable_message.model_copy(update={"ack": gate.request})
        completed = await self._run_gated_callback(gated_message, raise_err=raise_err)
        if completed:
            await self._settle_delivery(ackable_message, gate, raise_err=raise_err)

    async def _run_gated_callback(
        self,
        message: AckableMessage,
        *,
        raise_err: bool,
    ) -> bool:
        """Run Taskiq's callback and transfer failure ownership to the listener."""
        try:
            await Receiver.callback(self, message, raise_err=True)
        except asyncio.CancelledError:
            self._stop_consumer()
            raise
        except Exception:
            self._stop_consumer()
            if raise_err or self._finish_event is None:
                raise
            return False
        return True

    async def _settle_delivery(
        self,
        message: AckableMessage,
        gate: _AcknowledgementGate,
        *,
        raise_err: bool,
    ) -> None:
        """Settle exactly one Taskiq request or stop the consumer fail-closed."""
        if gate.requests != 1:
            self._stop_consumer()
            if raise_err or self._finish_event is None:
                raise RuntimeError(_ACK_REQUEST_COUNT_INVALID)
            return
        await self._commit_acknowledgement(message, raise_err=raise_err)

    async def _commit_acknowledgement(
        self,
        message: AckableMessage,
        *,
        raise_err: bool,
    ) -> None:
        """Commit the one deferred broker ACK and fail closed on ambiguity."""
        try:
            acknowledgement = message.ack()
            if inspect.isawaitable(acknowledgement):
                async with asyncio.timeout(self._acknowledgement_timeout):
                    await acknowledgement
        except asyncio.CancelledError:
            self._stop_consumer()
            raise
        except Exception:
            self._stop_consumer()
            if raise_err or self._finish_event is None:
                raise

    def _stop_consumer(self) -> None:
        """Signal first failure without retaining an exception or delivery payload."""
        first_failure = self._failure_event is None or not self._failure_event.is_set()
        if self._failure_event is not None:
            self._failure_event.set()
        if self._finish_event is not None:
            self._finish_event.set()
        if first_failure:
            _LOGGER.error(_WITHHELD_ACK_MESSAGE)
