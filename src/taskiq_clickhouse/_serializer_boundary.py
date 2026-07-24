"""Safe asynchronous boundary around synchronous Taskiq serializers."""

from __future__ import annotations

import asyncio
from contextvars import copy_context
from dataclasses import dataclass, field
from enum import Enum, auto
from functools import partial
import typing

from taskiq_clickhouse._executor_admission import SubmissionAdmission
from taskiq_clickhouse._executor_pool import ProcessThreadPool
from taskiq_clickhouse._mapping_snapshot import materialize_exact_mapping
from taskiq_clickhouse.exceptions import ClickHouseDecodeError, ClickHouseEncodeError


if typing.TYPE_CHECKING:
    from taskiq.abc.serializer import TaskiqSerializer


_BoundaryValueT = typing.TypeVar("_BoundaryValueT")
# Keep custom synchronous serializers away from asyncio's process-wide default
# executor. Serializers remain required to terminate: Python cannot stop a
# running thread, but a faulty serializer cannot starve unrelated to_thread use.
_BOUNDARY_EXECUTOR: typing.Final = ProcessThreadPool(
    thread_name_prefix="taskiq-clickhouse-serializer",
)


@dataclass(frozen=True, slots=True, repr=False)
class CapturedValue(typing.Generic[_BoundaryValueT]):
    """Carry a boundary value without retaining an exception or traceback."""

    boundary_value: _BoundaryValueT


class BoundaryFailure(Enum):
    """Classify value-free failures crossing a synchronous trust boundary."""

    CALL_FAILED = auto()
    EXECUTOR_UNAVAILABLE = auto()


BOUNDARY_UNAVAILABLE_REASON: typing.Final = "boundary_unavailable"

BoundaryOutcome: typing.TypeAlias = CapturedValue[_BoundaryValueT] | BoundaryFailure


@dataclass(frozen=True, slots=True)
class SerializerBoundary:
    """Run one configured Taskiq serializer outside the event-loop thread."""

    serializer: TaskiqSerializer = field(repr=False)
    admission: SubmissionAdmission = field(
        default_factory=SubmissionAdmission,
        repr=False,
    )

    async def dump(
        self,
        candidate: object,
        *,
        operation: str,
        failed_reason: str,
        type_reason: str,
    ) -> bytes:
        """Serialize a value and require exact immutable bytes."""
        outcome = await self._run(partial(_dump_serializer, self.serializer, candidate))
        if outcome is BoundaryFailure.EXECUTOR_UNAVAILABLE:
            raise ClickHouseEncodeError(operation, BOUNDARY_UNAVAILABLE_REASON) from None
        if outcome is BoundaryFailure.CALL_FAILED:
            raise ClickHouseEncodeError(operation, failed_reason) from None
        if type(outcome.boundary_value) is not bytes:  # noqa: WPS516 - reject bytes subclasses.
            raise ClickHouseEncodeError(operation, type_reason)
        return outcome.boundary_value

    async def load(
        self,
        payload: bytes,
        *,
        operation: str,
        failed_reason: str,
    ) -> object:
        """Deserialize bytes without exposing serializer exception details."""
        outcome = await self._run(partial(_load_serializer, self.serializer, payload))
        if outcome is BoundaryFailure.EXECUTOR_UNAVAILABLE:
            raise ClickHouseDecodeError(operation, BOUNDARY_UNAVAILABLE_REASON) from None
        if outcome is BoundaryFailure.CALL_FAILED:
            raise ClickHouseDecodeError(operation, failed_reason) from None
        return outcome.boundary_value

    async def _run(
        self,
        operation: typing.Callable[[], _BoundaryValueT],
    ) -> BoundaryOutcome[_BoundaryValueT]:
        permit = await self.admission.acquire()
        with permit:
            return await run_boundary(operation)


async def run_boundary(
    operation: typing.Callable[[], _BoundaryValueT],
    *,
    executor: ProcessThreadPool = _BOUNDARY_EXECUTOR,
) -> BoundaryOutcome[_BoundaryValueT]:
    """Own one synchronous job through outer cancellation and termination."""
    try:
        permit = await executor.acquire()
    except Exception:  # noqa: BLE001 - executor infrastructure has no safe public diagnostics.
        return BoundaryFailure.EXECUTOR_UNAVAILABLE
    with permit:
        try:
            caller_context = copy_context()
            boundary_future = asyncio.get_running_loop().run_in_executor(
                executor.executor,
                caller_context.run,
                capture_boundary,
                operation,
            )
        except Exception:  # noqa: BLE001 - pool creation and submission failures are classified below.
            return BoundaryFailure.EXECUTOR_UNAVAILABLE
        try:
            return await asyncio.shield(boundary_future)
        except asyncio.CancelledError:
            await _drain_boundary_future(boundary_future)
            raise
        except Exception:  # noqa: BLE001 - worker operations already capture their ordinary failures.
            return BoundaryFailure.EXECUTOR_UNAVAILABLE


async def _drain_boundary_future(
    boundary_future: asyncio.Future[CapturedValue[_BoundaryValueT] | BoundaryFailure],
) -> None:
    """Wait for the exact submitted job and surface its terminal fatal signal."""
    while not boundary_future.done():
        try:
            await asyncio.shield(boundary_future)
        except asyncio.CancelledError:
            continue
        except Exception:  # noqa: BLE001 - infrastructure failure is subordinate to caller cancellation.
            break
    if boundary_future.cancelled():
        return
    terminal_error = boundary_future.exception()
    if terminal_error is not None and not isinstance(terminal_error, Exception):
        raise terminal_error


async def copy_exact_mapping(
    candidate: object,
    field_names: tuple[str, ...],
    *,
    require_dict: bool,
) -> BoundaryOutcome[dict[str, object]]:
    """Copy one exact mapping shape outside the event-loop thread."""
    return await run_boundary(
        partial(
            materialize_exact_mapping,
            candidate,
            field_names,
            require_dict=require_dict,
        ),
    )


def _dump_serializer(serializer: TaskiqSerializer, candidate: object) -> bytes:
    """Resolve and invoke an untrusted serializer inside the worker boundary."""
    return serializer.dumpb(candidate)


def _load_serializer(serializer: TaskiqSerializer, payload: bytes) -> object:
    """Resolve and invoke an untrusted deserializer inside the worker boundary."""
    return serializer.loadb(payload)


def capture_boundary(
    operation: typing.Callable[[], _BoundaryValueT],
) -> CapturedValue[_BoundaryValueT] | BoundaryFailure:
    """Discard arbitrary ordinary exceptions before leaving a trust boundary."""
    try:
        return CapturedValue(operation())
    except asyncio.CancelledError:
        return BoundaryFailure.CALL_FAILED
    except Exception:  # noqa: BLE001 - model and serializer implementations are untrusted.
        return BoundaryFailure.CALL_FAILED
