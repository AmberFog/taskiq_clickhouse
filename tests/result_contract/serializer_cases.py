"""Taskiq serializer doubles for exact codec-boundary scenarios."""

from typing import Any

from taskiq.abc.serializer import TaskiqSerializer
from taskiq.serializers.json_serializer import JSONSerializer


class CountingSerializer(TaskiqSerializer):
    """Delegate JSON while exposing whether lifecycle checks reached it."""

    def __init__(self) -> None:
        """Create a zero-call serializer probe."""
        self._delegate = JSONSerializer()
        self.dump_calls = 0
        self.load_calls = 0

    def dumpb(self, candidate: Any) -> bytes:  # noqa: ANN401 - inherited Taskiq API.
        """Count and delegate one JSON dump."""
        self.dump_calls += 1
        return self._delegate.dumpb(candidate)

    def loadb(self, payload: bytes) -> Any:  # noqa: ANN401 - inherited Taskiq API.
        """Count and delegate one JSON load."""
        self.load_calls += 1
        return self._delegate.loadb(payload)


class FailingLoadSerializer(TaskiqSerializer):
    """Fail one configured load and expose whether fallback was attempted."""

    def __init__(self, detail: str) -> None:
        """Retain one hostile raw detail for the configured failure."""
        self.detail = detail
        self.load_calls = 0

    def dumpb(self, candidate: Any) -> bytes:  # noqa: ANN401 - inherited Taskiq API.
        """Return one inert payload for the unused encoding direction."""
        del candidate
        return b"configured"

    def loadb(self, payload: bytes) -> Any:  # noqa: ANN401 - inherited Taskiq API.
        """Raise the configured raw failure and count the only attempt."""
        del payload
        self.load_calls += 1
        raise RuntimeError(self.detail)
