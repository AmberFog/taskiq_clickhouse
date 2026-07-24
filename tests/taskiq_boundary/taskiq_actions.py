"""Build and exercise Taskiq receiver-boundary scenarios."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from taskiq import AckableMessage, InMemoryBroker, TaskiqMessage
from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.acks import AcknowledgeType
from taskiq.receiver import Receiver
from taskiq.result import TaskiqResult

from taskiq_clickhouse.backend import ClickHouseResultBackend
from tests.factories.receiver import AckableMessageFactory, Acknowledgement
from tests.taskiq_boundary import constants as boundary_constants
from tests.taskiq_boundary.models import ReceiverFailureCase


if TYPE_CHECKING:
    from taskiq.message import TaskiqMessage as ReceiverMessage


class _SecretResultLogMiddleware(TaskiqMiddleware):
    """Put a private log into the result before the failing save."""

    def post_execute(
        self,
        message: ReceiverMessage,
        result: TaskiqResult[Any],  # noqa: WPS110 - inherited Taskiq API.
    ) -> None:
        """Attach the sentinel log without logging it."""
        result.log = f"{boundary_constants.SECRET_TASK_LOG}:{message.task_id}"


def make_unstarted_backend() -> ClickHouseResultBackend[Any]:
    """Build a side-effect-free backend carrying inert private values."""
    return ClickHouseResultBackend(
        host=boundary_constants.UNIT_HOST,
        database="tasks",
        secure=False,
        username=boundary_constants.UNIT_USERNAME,
        password=boundary_constants.UNIT_PASSWORD,
        result_ttl=boundary_constants.RESULT_TTL,
        purge_ttl=boundary_constants.PURGE_TTL,
        namespace="taskiq-boundary-unit",
    )


def make_task_result() -> TaskiqResult[Any]:
    """Build a complete result for the direct failure comparison."""
    return TaskiqResult(
        is_err=False,
        log=boundary_constants.SECRET_TASK_LOG,
        return_value=boundary_constants.SECRET_RETURN_VALUE,
        execution_time=boundary_constants.DIRECT_EXECUTION_TIME,
        labels={"source": "direct"},
        error=None,
    )


def build_receiver_failure_case(
    backend: ClickHouseResultBackend[Any],
) -> ReceiverFailureCase:
    """Attach a NEW backend to a real receiver and one ackable task."""
    events: list[str] = []
    broker = InMemoryBroker(await_inplace=True).with_result_backend(backend)
    broker.with_middlewares(_SecretResultLogMiddleware())

    @broker.task(task_name=boundary_constants.RECEIVER_FAILURE_TASK)
    async def return_private_value() -> str:  # noqa: WPS430 - scenario closure owns event state.
        events.append("task")
        return boundary_constants.SECRET_RETURN_VALUE

    receiver = Receiver(
        broker=broker,
        max_async_tasks=1,
        ack_type=AcknowledgeType.WHEN_SAVED,
    )
    wire_message = broker.formatter.dumps(
        TaskiqMessage(
            task_id=boundary_constants.RECEIVER_FAILURE_TASK_ID,
            task_name=return_private_value.task_name,
            labels={},
            args=[],
            kwargs={},
        ),
    )

    async def acknowledge() -> None:  # noqa: WPS430 - ack closure owns observable events.
        events.append("ack")

    return ReceiverFailureCase(
        backend=backend,
        broker=broker,
        receiver=receiver,
        message=AckableMessage(data=wire_message.message, ack=acknowledge),
        events=events,
    )


def build_persistence_delivery(
    broker: InMemoryBroker,
    events: list[str],
    acknowledge: Acknowledgement,
) -> AckableMessage:
    """Register one observed task and serialize its ackable broker delivery."""

    @broker.task(task_name=boundary_constants.PERSISTENCE_RECEIVER_TASK)
    async def observed_task() -> str:  # noqa: WPS430 - scenario closure owns observable events.
        events.append("task")
        return "completed"

    wire_message = broker.formatter.dumps(
        TaskiqMessage(
            task_id=boundary_constants.PERSISTENCE_RECEIVER_TASK_ID,
            task_name=observed_task.task_name,
            labels={},
            args=[],
            kwargs={},
        ),
    )
    return AckableMessageFactory.build(data=wire_message.message, ack=acknowledge)


async def observe_direct_set_failure(
    backend: ClickHouseResultBackend[Any],
) -> None:
    """Call the same backend directly before observing receiver handling."""
    await backend.set_result(boundary_constants.DIRECT_FAILURE_TASK_ID, make_task_result())
