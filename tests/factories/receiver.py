"""Factories for the persistence-gated Taskiq receiver contract."""

from collections.abc import Awaitable, Callable
from typing import TypeAlias

from factory.base import Factory
from taskiq import AckableMessage
from taskiq.acks import AcknowledgeType

from taskiq_clickhouse.receiver import ResultPersistenceReceiver


Acknowledgement: TypeAlias = Callable[[], None | Awaitable[None]]


class ResultPersistenceReceiverFactory(Factory[ResultPersistenceReceiver]):
    """Build a valid receiver while exposing each guarded constructor axis."""

    class Meta:
        """Bind this factory to the exact public receiver type."""

        model = ResultPersistenceReceiver

    max_async_tasks = 1
    max_async_tasks_jitter = 0
    max_prefetch = 1
    ack_type = AcknowledgeType.WHEN_SAVED
    wait_tasks_timeout = 1.0


class AckableMessageFactory(Factory[AckableMessage]):
    """Build one ackable broker delivery with an explicit settlement callable."""

    class Meta:
        """Bind this factory to Taskiq's exact delivery type."""

        model = AckableMessage

    data = b"taskiq-message"
