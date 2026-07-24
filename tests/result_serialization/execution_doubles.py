"""Thread-aware serializer doubles for result execution-boundary tests."""

from threading import Barrier, Event, Lock, get_ident

from taskiq.abc.serializer import TaskiqSerializer
from taskiq.serializers.pickle import PickleSerializer


class ThreadProbeSerializer(TaskiqSerializer):
    """Record every serializer call's executor thread."""

    def __init__(self) -> None:
        """Create a probe with separate dump and load observations."""
        self.delegate = PickleSerializer()
        self.dump_thread_ids: list[int] = []
        self.load_thread_ids: list[int] = []

    def dumpb(self, candidate: object) -> bytes:
        """Record the current thread before serialization."""
        self.dump_thread_ids.append(get_ident())
        return self.delegate.dumpb(candidate)

    def loadb(self, payload: bytes) -> object:
        """Record the current thread before deserialization."""
        self.load_thread_ids.append(get_ident())
        return self.delegate.loadb(payload)


class BlockingSerializer(TaskiqSerializer):
    """Block the first executor dump until a cancellation test releases it."""

    def __init__(
        self,
        started: Event,
        release: Event,
        *,
        terminal_error: BaseException | None = None,
    ) -> None:
        """Configure deterministic start, release and terminal signals."""
        self.started = started
        self.release = release
        self.terminal_error = terminal_error
        self.delegate = PickleSerializer()
        self.calls = 0

    def dumpb(self, candidate: object) -> bytes:
        """Expose a deterministic running-job boundary."""
        self.calls += 1
        self.started.set()
        self.release.wait()
        if self.terminal_error is not None:
            raise self.terminal_error
        return self.delegate.dumpb(candidate)

    def loadb(self, payload: bytes) -> object:
        """Delegate deserialization without blocking."""
        return self.delegate.loadb(payload)


class ConcurrentSerializer(TaskiqSerializer):
    """Require two codec instances to overlap only inside serializer work."""

    def __init__(self) -> None:
        """Create a two-party overlap barrier and synchronized counters."""
        self.delegate = PickleSerializer()
        self.start_barrier = Barrier(2)
        self.active_barrier = Barrier(2)
        self.lock = Lock()
        self.active = 0
        self.max_active = 0

    def dumpb(self, candidate: object) -> bytes:
        """Measure real overlap after both executor jobs are running."""
        self.start_barrier.wait(timeout=2)
        with self.lock:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
        try:
            self.active_barrier.wait(timeout=2)
            return self.delegate.dumpb(candidate)
        finally:
            with self.lock:
                self.active -= 1

    def loadb(self, payload: bytes) -> object:
        """Delegate deserialization without synchronization."""
        return self.delegate.loadb(payload)


class FatalBoundarySignal(BaseException):
    """Synthetic process-level signal that the boundary must preserve."""
