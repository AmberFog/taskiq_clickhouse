"""Shared doubles and assertions for serializer boundary tests."""

# ruff: noqa: S101

from collections.abc import Iterator, Mapping
from typing import cast

from taskiq.abc.serializer import TaskiqSerializer
from taskiq.serializers.pickle import PickleSerializer

from taskiq_clickhouse._serializer_boundary import BoundaryFailure
from taskiq_clickhouse.exceptions import ClickHouseResultBackendError


SERIALIZER_FAILURE_DETAIL = "private-serializer-failure-detail"


class RecordingSerializer(TaskiqSerializer):
    """Record Python values while delegating bytes to Taskiq Pickle."""

    def __init__(self) -> None:
        """Initialize empty call observations."""
        self.delegate = PickleSerializer()
        self.dumped: list[object] = []
        self.loaded: list[bytes] = []

    def dumpb(self, candidate: object) -> bytes:
        """Record and serialize one candidate."""
        self.dumped.append(candidate)
        return self.delegate.dumpb(candidate)

    def loadb(self, payload: bytes) -> object:
        """Record and deserialize one payload."""
        self.loaded.append(payload)
        return self.delegate.loadb(payload)


class ScriptedSerializer(TaskiqSerializer):
    """Return or raise deterministic boundary events in call order."""

    def __init__(
        self,
        *,
        dump_events: list[object] | None = None,
        load_events: list[object] | None = None,
    ) -> None:
        """Initialize isolated event queues and call observations."""
        self.dump_events = [] if dump_events is None else dump_events
        self.load_events = [] if load_events is None else load_events
        self.dumped: list[object] = []
        self.loaded: list[bytes] = []

    def dumpb(self, candidate: object) -> bytes:
        """Consume one scripted dump event."""
        self.dumped.append(candidate)
        event = self.dump_events.pop(0)
        if isinstance(event, BaseException):
            raise event
        return cast("bytes", event)

    def loadb(self, payload: bytes) -> object:
        """Consume one scripted load event."""
        self.loaded.append(payload)
        event = self.load_events.pop(0)
        if isinstance(event, BaseException):
            raise event
        return event


class ExplodingMapping(Mapping[str, object]):
    """Behave as a mapping until copying attempts to iterate it."""

    def __getitem__(self, key: str) -> object:
        """Reject item access with a recognizable private detail."""
        del key
        raise RuntimeError(SERIALIZER_FAILURE_DETAIL)

    def __iter__(self) -> Iterator[str]:
        """Reject iteration with a recognizable private detail."""
        raise RuntimeError(SERIALIZER_FAILURE_DETAIL)

    def __len__(self) -> int:
        """Advertise one item so consumers attempt iteration."""
        return 1


class BytesSubclass(bytes):
    """Distinguish exact immutable bytes from subclasses."""


def assert_safe_error(
    error: ClickHouseResultBackendError,
    *,
    operation: str,
    reason: str,
) -> None:
    """Assert stable diagnostics without exposing the raw boundary detail."""
    assert error.operation == operation
    assert error.reason == reason
    assert error.__cause__ is None
    assert error.__context__ is None
    assert SERIALIZER_FAILURE_DETAIL not in str(error)
    assert SERIALIZER_FAILURE_DETAIL not in repr(error)


async def boundary_unavailable(*args: object, **kwargs: object) -> BoundaryFailure:
    """Represent unavailable executor admission without running work."""
    del args, kwargs
    return BoundaryFailure.EXECUTOR_UNAVAILABLE
