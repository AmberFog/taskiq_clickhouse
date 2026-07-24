"""Protect the persistence-gated Taskiq receiver contract."""

import asyncio
from collections.abc import AsyncIterator
import math
from typing import Any, cast

import pytest
from taskiq import AckableMessage, InMemoryBroker
from taskiq.acks import AcknowledgeType
from taskiq.receiver import Receiver
from taskiq.result import TaskiqResult

from taskiq_clickhouse import ResultPersistenceReceiver
from tests.factories.receiver import (
    AckableMessageFactory,
    ResultPersistenceReceiverFactory,
)
from tests.taskiq_boundary import constants as boundary_constants
from tests.taskiq_boundary.doubles import PostSaveProbe
from tests.taskiq_boundary.taskiq_actions import build_persistence_delivery


pytestmark = pytest.mark.asyncio

_CONCURRENT_CALLBACKS = 2
_STOPPED_REASON = "result_persistence_receiver_stopped"
_REUSED_RECEIVER = "ResultPersistenceReceiver can listen only once."
_ACK_REQUEST_COUNT_INVALID = "Taskiq requested an unexpected number of acknowledgements."
_ACKABLE_MESSAGE_REQUIRED = "ResultPersistenceReceiver requires broker messages with explicit acknowledgement."
_INVALID_ACK_TYPE = "ResultPersistenceReceiver requires the WHEN_SAVED acknowledgement mode."
_INVALID_WAIT_TIMEOUT = "ResultPersistenceReceiver requires a finite positive wait_tasks_timeout."
_INVALID_MAX_ASYNC_TASKS = "ResultPersistenceReceiver requires max_async_tasks=1."
_INVALID_MAX_ASYNC_TASKS_JITTER = "ResultPersistenceReceiver requires max_async_tasks_jitter=0."
_INVALID_MAX_PREFETCH = "ResultPersistenceReceiver requires max_prefetch=1."
_WITHHELD_ACK_MESSAGE = (
    "Task processing did not reach confirmed broker settlement; "
    "acknowledgement was withheld or its outcome is ambiguous, "
    "and the worker consumer is stopping."
)


class _PersistenceError(RuntimeError):
    """Distinct failure used to assert exception identity."""


class _AcknowledgementError(RuntimeError):
    """Distinct broker failure used to assert exception identity."""


@pytest.mark.parametrize(
    "wait_tasks_timeout",
    [None, True, False, "1", 0.0, -1.0, math.inf, -math.inf, math.nan, 10**1000],
)
async def test_constructor_rejects_unbounded_or_invalid_drain_timeout(
    broker: InMemoryBroker,
    wait_tasks_timeout: object,
) -> None:
    """Require a real, finite, positive shutdown bound."""
    invalid_timeout = cast("float | None", wait_tasks_timeout)

    with pytest.raises(ValueError, match="finite positive") as raised:
        ResultPersistenceReceiverFactory.build(broker=broker, wait_tasks_timeout=invalid_timeout)

    assert str(raised.value) == _INVALID_WAIT_TIMEOUT


@pytest.mark.parametrize("ack_type", [None, AcknowledgeType.WHEN_SAVED])
async def test_constructor_normalizes_only_the_persistence_ack_mode(
    broker: InMemoryBroker,
    ack_type: AcknowledgeType | None,
) -> None:
    """Accept the default and explicit persistence-gated ACK modes."""
    receiver = ResultPersistenceReceiverFactory.build(broker=broker, ack_type=ack_type)

    assert receiver.ack_time is AcknowledgeType.WHEN_SAVED
    assert receiver.wait_tasks_timeout == 1.0


@pytest.mark.parametrize(
    "ack_type",
    [AcknowledgeType.WHEN_RECEIVED, AcknowledgeType.WHEN_EXECUTED],
)
async def test_constructor_rejects_early_ack_modes(
    broker: InMemoryBroker,
    ack_type: AcknowledgeType,
) -> None:
    """Prevent configuration from silently weakening persistence gating."""
    with pytest.raises(ValueError, match="WHEN_SAVED") as raised:
        ResultPersistenceReceiverFactory.build(broker=broker, ack_type=ack_type)

    assert str(raised.value) == _INVALID_ACK_TYPE


async def test_constructor_rejects_string_that_only_compares_equal_to_ack_enum(
    broker: InMemoryBroker,
) -> None:
    """Require the typed Taskiq mode rather than accepting str-enum equality."""
    string_ack_type = cast("AcknowledgeType", "when_saved")

    with pytest.raises(ValueError, match="WHEN_SAVED") as raised:
        ResultPersistenceReceiverFactory.build(broker=broker, ack_type=string_ack_type)

    assert str(raised.value) == _INVALID_ACK_TYPE


@pytest.mark.parametrize("max_async_tasks", [None, True, False, 0, 2, 1.0, "1"])
async def test_constructor_requires_one_in_flight_callback(
    broker: InMemoryBroker,
    max_async_tasks: object,
) -> None:
    """Prevent sibling callbacks from outliving a persistence-triggered stop."""
    invalid_limit = cast("int | None", max_async_tasks)

    with pytest.raises(ValueError, match="max_async_tasks=1") as raised:
        ResultPersistenceReceiverFactory.build(broker=broker, max_async_tasks=invalid_limit)

    assert str(raised.value) == _INVALID_MAX_ASYNC_TASKS


@pytest.mark.parametrize("max_async_tasks_jitter", [True, False, -1, 1, 0.0, "0"])
async def test_constructor_disables_callback_limit_jitter(
    broker: InMemoryBroker,
    max_async_tasks_jitter: object,
) -> None:
    """Keep the one-callback shutdown invariant deterministic."""
    invalid_jitter = cast("int", max_async_tasks_jitter)

    with pytest.raises(ValueError, match="max_async_tasks_jitter=0") as raised:
        ResultPersistenceReceiverFactory.build(broker=broker, max_async_tasks_jitter=invalid_jitter)

    assert str(raised.value) == _INVALID_MAX_ASYNC_TASKS_JITTER


@pytest.mark.parametrize("max_prefetch", [True, False, -1, 0, 2, 1.0, "1"])
async def test_constructor_requires_one_prefetched_delivery(
    broker: InMemoryBroker,
    max_prefetch: object,
) -> None:
    """Bound the number of unsettled deliveries owned by one child process."""
    invalid_prefetch = cast("int", max_prefetch)

    with pytest.raises(ValueError, match="max_prefetch=1") as raised:
        ResultPersistenceReceiverFactory.build(broker=broker, max_prefetch=invalid_prefetch)

    assert str(raised.value) == _INVALID_MAX_PREFETCH


async def test_real_taskiq_callback_commits_one_ack_after_post_save(
    broker: InMemoryBroker,
    result_persistence_receiver: ResultPersistenceReceiver,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Confirm the installed Taskiq callback completes its save phase first."""
    events: list[str] = []
    original_set_result = broker.result_backend.set_result

    async def observed_set_result(
        task_id: str,
        result: TaskiqResult[Any],
    ) -> None:
        events.append("set_result")
        await original_set_result(task_id, result)

    async def acknowledge() -> None:
        events.append("ack")

    monkeypatch.setattr(broker.result_backend, "set_result", observed_set_result)
    broker.with_middlewares(PostSaveProbe(events))
    delivery = build_persistence_delivery(broker, events, acknowledge)
    original_acknowledgement = delivery.ack

    await result_persistence_receiver.callback(delivery)

    assert events == ["task", "set_result", "post_save", "ack"]
    assert delivery.ack is original_acknowledgement


async def test_real_persistence_failure_is_propagated_without_ack_or_post_save(
    broker: InMemoryBroker,
    result_persistence_receiver: ResultPersistenceReceiver,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep a failed result write unacknowledged and diagnostically exact."""
    events: list[str] = []
    failure = _PersistenceError("persistence-failed")

    async def fail_set_result(
        _task_id: str,
        _result: TaskiqResult[Any],
    ) -> None:
        events.append("set_result")
        raise failure

    async def acknowledge() -> None:
        events.append("ack")

    monkeypatch.setattr(broker.result_backend, "set_result", fail_set_result)
    broker.with_middlewares(PostSaveProbe(events))
    delivery = build_persistence_delivery(broker, events, acknowledge)

    with pytest.raises(_PersistenceError) as raised:
        await result_persistence_receiver.callback(delivery)

    assert raised.value is failure
    assert events == ["task", "set_result"]


async def test_post_save_failure_withholds_ack_after_durable_result(
    broker: InMemoryBroker,
    result_persistence_receiver: ResultPersistenceReceiver,
) -> None:
    """Expose the deliberate at-least-once outcome of a post-save failure."""
    events: list[str] = []
    failure = _PersistenceError("post-save-failed")

    async def acknowledge() -> None:
        events.append("ack")

    broker.with_middlewares(PostSaveProbe(events, error=failure))
    delivery = build_persistence_delivery(broker, events, acknowledge)

    with pytest.raises(_PersistenceError) as raised:
        await result_persistence_receiver.callback(delivery)

    assert raised.value is failure
    assert events == ["task", "post_save"]
    assert await broker.result_backend.is_result_ready(boundary_constants.PERSISTENCE_RECEIVER_TASK_ID) is True


async def test_non_ackable_delivery_is_rejected_before_taskiq_execution(
    result_persistence_receiver: ResultPersistenceReceiver,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fail closed when a broker cannot expose explicit settlement."""
    parent_called = False

    async def parent_callback(
        _receiver: Receiver,
        _message: bytes | AckableMessage,
        raise_err: bool = False,  # noqa: FBT001, FBT002 - mirrors patched Taskiq API.
    ) -> None:
        del raise_err
        nonlocal parent_called
        parent_called = True

    monkeypatch.setattr(Receiver, "callback", parent_callback)

    with pytest.raises(TypeError) as raised:
        await result_persistence_receiver.callback(b"not-ackable")

    assert str(raised.value) == _ACKABLE_MESSAGE_REQUIRED
    assert parent_called is False


async def test_sync_broker_ack_is_committed_once(
    result_persistence_receiver: ResultPersistenceReceiver,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Support Taskiq's synchronous acknowledgement callable contract."""
    events: list[str] = []

    async def parent_callback(
        _receiver: Receiver,
        message: bytes | AckableMessage,
        raise_err: bool = False,  # noqa: FBT001, FBT002 - mirrors patched Taskiq API.
    ) -> None:
        del raise_err
        assert isinstance(message, AckableMessage)
        events.append("save_phase")
        message.ack()

    def acknowledge() -> None:
        events.append("ack")

    monkeypatch.setattr(Receiver, "callback", parent_callback)

    await result_persistence_receiver.callback(
        AckableMessageFactory.build(ack=acknowledge),
    )

    assert events == ["save_phase", "ack"]


@pytest.mark.parametrize("requested_ack_count", [0, 2])
async def test_unexpected_taskiq_ack_count_fails_closed(
    result_persistence_receiver: ResultPersistenceReceiver,
    monkeypatch: pytest.MonkeyPatch,
    requested_ack_count: int,
) -> None:
    """Reject missing or repeated settlement requests without broker ACK."""
    broker_ack_count = 0

    async def parent_callback(
        _receiver: Receiver,
        message: bytes | AckableMessage,
        raise_err: bool = False,  # noqa: FBT001, FBT002 - mirrors patched Taskiq API.
    ) -> None:
        del raise_err
        assert isinstance(message, AckableMessage)
        for _ in range(requested_ack_count):
            message.ack()

    def acknowledge() -> None:
        nonlocal broker_ack_count
        broker_ack_count += 1

    monkeypatch.setattr(Receiver, "callback", parent_callback)

    with pytest.raises(RuntimeError) as raised:
        await result_persistence_receiver.callback(
            AckableMessageFactory.build(ack=acknowledge),
        )

    assert str(raised.value) == _ACK_REQUEST_COUNT_INVALID
    assert broker_ack_count == 0


async def test_direct_broker_ack_failure_preserves_exception_identity(
    result_persistence_receiver: ResultPersistenceReceiver,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Expose an ambiguous broker outcome instead of swallowing it."""
    failure = _AcknowledgementError("ack-failed")
    acknowledgement_attempts = 0

    async def parent_callback(
        _receiver: Receiver,
        message: bytes | AckableMessage,
        raise_err: bool = False,  # noqa: FBT001, FBT002 - mirrors patched Taskiq API.
    ) -> None:
        del raise_err
        assert isinstance(message, AckableMessage)
        message.ack()

    async def acknowledge() -> None:
        nonlocal acknowledgement_attempts
        acknowledgement_attempts += 1
        raise failure

    monkeypatch.setattr(Receiver, "callback", parent_callback)

    with pytest.raises(_AcknowledgementError) as raised:
        await result_persistence_receiver.callback(
            AckableMessageFactory.build(ack=acknowledge),
        )

    assert raised.value is failure
    assert acknowledgement_attempts == 1


async def test_awaitable_broker_ack_is_bounded_by_drain_timeout(
    broker: InMemoryBroker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Stop on an indefinitely pending ACK instead of hanging the receiver."""
    acknowledgement_started = asyncio.Event()
    never_complete = asyncio.Event()

    async def parent_callback(
        _receiver: Receiver,
        message: bytes | AckableMessage,
        raise_err: bool = False,  # noqa: FBT001, FBT002 - mirrors patched Taskiq API.
    ) -> None:
        del raise_err
        assert isinstance(message, AckableMessage)
        message.ack()

    async def acknowledge() -> None:
        acknowledgement_started.set()
        await never_complete.wait()

    monkeypatch.setattr(Receiver, "callback", parent_callback)

    with pytest.raises(TimeoutError):
        await ResultPersistenceReceiverFactory.build(
            broker=broker,
            wait_tasks_timeout=0.01,
        ).callback(AckableMessageFactory.build(ack=acknowledge))

    assert acknowledgement_started.is_set()


async def test_taskiq_callback_cancellation_is_propagated_without_ack(
    result_persistence_receiver: ResultPersistenceReceiver,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Preserve cancellation identity before broker settlement."""
    cancellation = asyncio.CancelledError("callback-cancelled")
    acknowledged = False

    async def parent_callback(
        _receiver: Receiver,
        _message: bytes | AckableMessage,
        raise_err: bool = False,  # noqa: FBT001, FBT002 - mirrors patched Taskiq API.
    ) -> None:
        del raise_err
        raise cancellation

    def acknowledge() -> None:
        nonlocal acknowledged
        acknowledged = True

    monkeypatch.setattr(Receiver, "callback", parent_callback)

    with pytest.raises(asyncio.CancelledError) as raised:
        await result_persistence_receiver.callback(
            AckableMessageFactory.build(ack=acknowledge),
        )

    assert raised.value is cancellation
    assert acknowledged is False


async def test_broker_ack_cancellation_is_propagated_after_one_attempt(
    result_persistence_receiver: ResultPersistenceReceiver,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Treat cancellation during ACK as one ambiguous settlement attempt."""
    cancellation = asyncio.CancelledError("ack-cancelled")
    acknowledgement_attempts = 0

    async def parent_callback(
        _receiver: Receiver,
        message: bytes | AckableMessage,
        raise_err: bool = False,  # noqa: FBT001, FBT002 - mirrors patched Taskiq API.
    ) -> None:
        del raise_err
        assert isinstance(message, AckableMessage)
        message.ack()

    async def acknowledge() -> None:
        nonlocal acknowledgement_attempts
        acknowledgement_attempts += 1
        raise cancellation

    monkeypatch.setattr(Receiver, "callback", parent_callback)

    with pytest.raises(asyncio.CancelledError) as raised:
        await result_persistence_receiver.callback(
            AckableMessageFactory.build(ack=acknowledge),
        )

    assert raised.value is cancellation
    assert acknowledgement_attempts == 1


async def test_real_listener_stops_admission_after_first_callback_failure(
    broker: InMemoryBroker,
    result_persistence_receiver: ResultPersistenceReceiver,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Keep a prefetched sibling unexecuted and unacknowledged after failure."""
    persistence_failure = _PersistenceError("listener-persistence-failed")
    second_delivery_prefetched = asyncio.Event()
    hold_broker_iterator = asyncio.Event()
    callback_attempts = 0
    first_ack_count = 0
    second_ack_count = 0

    async def parent_callback(
        _receiver: Receiver,
        message: bytes | AckableMessage,
        raise_err: bool = False,  # noqa: FBT001, FBT002 - mirrors patched Taskiq API.
    ) -> None:
        del raise_err
        nonlocal callback_attempts
        callback_attempts += 1
        assert isinstance(message, AckableMessage)
        await second_delivery_prefetched.wait()
        raise persistence_failure

    async def acknowledge_first() -> None:
        nonlocal first_ack_count
        first_ack_count += 1

    async def acknowledge_second() -> None:
        nonlocal second_ack_count
        second_ack_count += 1

    async def controlled_deliveries() -> AsyncIterator[AckableMessage]:
        yield AckableMessageFactory.build(ack=acknowledge_first)
        yield AckableMessageFactory.build(ack=acknowledge_second)
        second_delivery_prefetched.set()
        await hold_broker_iterator.wait()

    finish_event = asyncio.Event()
    monkeypatch.setattr(Receiver, "callback", parent_callback)
    monkeypatch.setattr(broker, "listen", controlled_deliveries)

    with (
        caplog.at_level("ERROR", logger="taskiq_clickhouse.receiver"),
        pytest.raises(RuntimeError) as raised,
    ):
        await asyncio.wait_for(
            result_persistence_receiver.listen(finish_event),
            timeout=1.0,
        )

    assert str(raised.value) == _STOPPED_REASON
    assert finish_event.is_set()
    assert callback_attempts == 1
    assert first_ack_count == 0
    assert second_ack_count == 0
    matching_records = [record for record in caplog.records if record.getMessage() == _WITHHELD_ACK_MESSAGE]
    assert len(matching_records) == 1


async def test_stopped_listener_rejects_diagnostic_callback_before_execution(
    result_persistence_receiver: ResultPersistenceReceiver,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Expose the stable stop reason when diagnostics request another callback."""
    failure = _PersistenceError("listener-persistence-failed")
    callback_attempts = 0

    async def parent_callback(
        _receiver: Receiver,
        _message: bytes | AckableMessage,
        raise_err: bool = False,  # noqa: FBT001, FBT002 - mirrors patched Taskiq API.
    ) -> None:
        del raise_err
        nonlocal callback_attempts
        callback_attempts += 1
        raise failure

    async def parent_listen(receiver: Receiver, _finish_event: asyncio.Event) -> None:
        await receiver.callback(AckableMessageFactory.build(ack=lambda: None))
        await receiver.callback(AckableMessageFactory.build(ack=lambda: None), raise_err=True)

    monkeypatch.setattr(Receiver, "callback", parent_callback)
    monkeypatch.setattr(Receiver, "listen", parent_listen)

    with pytest.raises(RuntimeError) as raised:
        await result_persistence_receiver.listen(asyncio.Event())

    assert str(raised.value) == _STOPPED_REASON
    assert callback_attempts == 1


async def test_listener_stops_when_taskiq_does_not_request_one_ack(
    result_persistence_receiver: ResultPersistenceReceiver,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Convert an invalid Taskiq settlement sequence into one consumer stop."""

    async def parent_callback(
        _receiver: Receiver,
        _message: bytes | AckableMessage,
        raise_err: bool = False,  # noqa: FBT001, FBT002 - mirrors patched Taskiq API.
    ) -> None:
        del raise_err

    async def parent_listen(receiver: Receiver, _finish_event: asyncio.Event) -> None:
        await receiver.callback(AckableMessageFactory.build(ack=lambda: None))

    monkeypatch.setattr(Receiver, "callback", parent_callback)
    monkeypatch.setattr(Receiver, "listen", parent_listen)

    with pytest.raises(RuntimeError) as raised:
        await result_persistence_receiver.listen(asyncio.Event())

    assert str(raised.value) == _STOPPED_REASON


async def test_listener_stops_after_ambiguous_broker_ack_failure(
    result_persistence_receiver: ResultPersistenceReceiver,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Suppress the broker error only until listener ownership can terminate."""
    failure = _AcknowledgementError("listener-ack-failed")

    async def parent_callback(
        _receiver: Receiver,
        message: bytes | AckableMessage,
        raise_err: bool = False,  # noqa: FBT001, FBT002 - mirrors patched Taskiq API.
    ) -> None:
        del raise_err
        assert isinstance(message, AckableMessage)
        message.ack()

    async def acknowledge() -> None:
        raise failure

    async def parent_listen(receiver: Receiver, _finish_event: asyncio.Event) -> None:
        await receiver.callback(AckableMessageFactory.build(ack=acknowledge))

    monkeypatch.setattr(Receiver, "callback", parent_callback)
    monkeypatch.setattr(Receiver, "listen", parent_listen)

    with pytest.raises(RuntimeError) as raised:
        await result_persistence_receiver.listen(asyncio.Event())

    assert str(raised.value) == _STOPPED_REASON


async def test_listener_logs_only_first_of_concurrent_stop_signals(
    result_persistence_receiver: ResultPersistenceReceiver,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Keep diagnostics bounded if a custom listener already admitted siblings."""
    both_callbacks_started = asyncio.Event()
    failure = _PersistenceError("concurrent-listener-failure")
    callback_attempts = 0

    async def parent_callback(
        _receiver: Receiver,
        _message: bytes | AckableMessage,
        raise_err: bool = False,  # noqa: FBT001, FBT002 - mirrors patched Taskiq API.
    ) -> None:
        del raise_err
        nonlocal callback_attempts
        callback_attempts += 1
        if callback_attempts == _CONCURRENT_CALLBACKS:
            both_callbacks_started.set()
        await both_callbacks_started.wait()
        raise failure

    async def parent_listen(receiver: Receiver, _finish_event: asyncio.Event) -> None:
        await asyncio.gather(
            receiver.callback(AckableMessageFactory.build(ack=lambda: None)),
            receiver.callback(AckableMessageFactory.build(ack=lambda: None)),
        )

    monkeypatch.setattr(Receiver, "callback", parent_callback)
    monkeypatch.setattr(Receiver, "listen", parent_listen)

    with (
        caplog.at_level("ERROR", logger="taskiq_clickhouse.receiver"),
        pytest.raises(RuntimeError) as raised,
    ):
        await result_persistence_receiver.listen(asyncio.Event())

    assert str(raised.value) == _STOPPED_REASON
    assert callback_attempts == _CONCURRENT_CALLBACKS
    matching_records = [record for record in caplog.records if record.getMessage() == _WITHHELD_ACK_MESSAGE]
    assert len(matching_records) == 1


async def test_raise_err_propagates_through_active_listener(
    result_persistence_receiver: ResultPersistenceReceiver,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retain Taskiq's explicit diagnostic callback mode during shutdown."""
    failure = _PersistenceError("diagnostic-listener-failed")
    finish_event = asyncio.Event()

    async def parent_callback(
        _receiver: Receiver,
        _message: bytes | AckableMessage,
        raise_err: bool = False,  # noqa: FBT001, FBT002 - mirrors patched Taskiq API.
    ) -> None:
        del raise_err
        raise failure

    async def parent_listen(receiver: Receiver, _finish_event: asyncio.Event) -> None:
        await receiver.callback(AckableMessageFactory.build(ack=lambda: None), raise_err=True)

    monkeypatch.setattr(Receiver, "callback", parent_callback)
    monkeypatch.setattr(Receiver, "listen", parent_listen)

    with pytest.raises(_PersistenceError) as raised:
        await result_persistence_receiver.listen(finish_event)

    assert raised.value is failure
    assert finish_event.is_set()


async def test_concurrent_listen_is_rejected_and_receiver_remains_one_shot(
    result_persistence_receiver: ResultPersistenceReceiver,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep listener lifecycle single-owner without resetting Taskiq internals."""
    entered = asyncio.Event()
    release = asyncio.Event()

    async def parent_listen(_receiver: Receiver, _finish_event: asyncio.Event) -> None:
        entered.set()
        await release.wait()

    monkeypatch.setattr(Receiver, "listen", parent_listen)
    first_listen = asyncio.create_task(result_persistence_receiver.listen(asyncio.Event()))
    await entered.wait()
    try:
        with pytest.raises(RuntimeError, match="cannot listen concurrently"):
            await result_persistence_receiver.listen(asyncio.Event())
    finally:
        release.set()
        await first_listen

    with pytest.raises(RuntimeError) as raised:
        await result_persistence_receiver.listen(asyncio.Event())

    assert str(raised.value) == _REUSED_RECEIVER


async def test_real_listener_cannot_reuse_consumed_taskiq_semaphores(
    broker: InMemoryBroker,
    result_persistence_receiver: ResultPersistenceReceiver,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reject a second listen after Taskiq's real runner consumes its semaphore."""
    hold_iterator = asyncio.Event()

    async def controlled_deliveries() -> AsyncIterator[AckableMessage]:
        await hold_iterator.wait()
        yield AckableMessageFactory.build(ack=lambda: None)

    monkeypatch.setattr(broker, "listen", controlled_deliveries)
    finish_event = asyncio.Event()
    finish_event.set()
    await asyncio.wait_for(result_persistence_receiver.listen(finish_event), timeout=1.0)

    with pytest.raises(RuntimeError) as raised:
        await result_persistence_receiver.listen(asyncio.Event())

    assert str(raised.value) == _REUSED_RECEIVER
