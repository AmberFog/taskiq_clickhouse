"""Verify executable documentation sources and local Markdown references."""

from dataclasses import dataclass
from pathlib import Path
import re
from textwrap import indent

import pytest
from taskiq import InMemoryBroker
from taskiq.abc.result_backend import AsyncResultBackend
from taskiq.result import TaskiqResult


_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_MARKDOWN_DOCUMENTS = (
    _PROJECT_ROOT / "README.md",
    *sorted((_PROJECT_ROOT / "docs").glob("*.md")),
)
_EXAMPLE_MODULES = tuple(sorted((_PROJECT_ROOT / "docs" / "examples").glob("*.py")))
_FENCE_PATTERN = re.compile(r"^```(?P<language>[^\s`]*)\s*$")
_LINK_PATTERN = re.compile(r"(?<!!)\[[^]]+\]\((?P<target>[^\n)]+)\)")
_HEADING_PATTERN = re.compile(r"^#{1,6}\s+(?P<heading>.+?)\s*#*\s*$")
_ANCHOR_CHARACTERS = re.compile(r"[^\w\- ]")
_INLINE_CODE_PATTERN = re.compile(r"`[^`]*`")


@dataclass(frozen=True, slots=True)
class _CodeFence:
    """Identify one fenced code block and its source location."""

    path: Path
    line_number: int
    language: str
    source: str


@dataclass(slots=True)
class _LifecycleProbeBackend(AsyncResultBackend[int]):
    """Record whether an InMemory receiver reaches a started backend."""

    events: list[str]
    started: bool = False

    async def startup(self) -> None:
        """Record explicit backend startup."""
        self.started = True
        self.events.append("backend_started")

    async def shutdown(self) -> None:
        """Record explicit backend shutdown."""
        self.started = False
        self.events.append("backend_stopped")

    async def set_result(  # noqa: WPS615 - inherited Taskiq API.
        self,
        task_id: str,
        result: TaskiqResult[int],  # noqa: WPS110 - inherited Taskiq API.
    ) -> None:
        """Require startup before accepting one receiver result."""
        del task_id, result
        if not self.started:
            message = "result backend was not started explicitly"
            raise AssertionError(message)
        self.events.append("result_stored")

    async def is_result_ready(self, task_id: str) -> bool:
        """Reject reads that are outside this lifecycle probe."""
        del task_id
        message = "readiness is outside the lifecycle probe"
        raise AssertionError(message)

    async def get_result(  # noqa: WPS463, WPS615 - inherited Taskiq API.
        self,
        task_id: str,
        with_logs: bool = False,  # noqa: FBT001, FBT002 - inherited Taskiq API.
    ) -> TaskiqResult[int]:
        """Reject reads that are outside this lifecycle probe."""
        del task_id, with_logs
        message = "result reads are outside the lifecycle probe"
        raise AssertionError(message)


def _code_fences(path: Path) -> tuple[_CodeFence, ...]:
    """Extract closed fenced blocks from one Markdown document."""
    fences: list[_CodeFence] = []
    language: str | None = None
    opening_line = 0
    lines: list[str] = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        marker = _FENCE_PATTERN.fullmatch(line)
        if marker is None:
            if language is not None:
                lines.append(line)
            continue
        if language is None:
            language = marker.group("language")
            opening_line = line_number
            lines = []
            continue
        if marker.group("language"):
            continue
        fences.append(_CodeFence(path, opening_line, language, "\n".join(lines)))
        language = None
    if language is not None:
        pytest.fail(f"unclosed code fence at {path}:{opening_line}")
    return tuple(fences)


def _python_fences() -> tuple[_CodeFence, ...]:
    """Return every Python example from maintained Markdown documents."""
    return tuple(
        fence
        for document in _MARKDOWN_DOCUMENTS
        for fence in _code_fences(document)
        if fence.language in {"py", "python"}
    )


def _local_links(path: Path) -> tuple[str, ...]:
    """Return local link targets while excluding external protocols."""
    targets = (match.group("target").strip().strip("<>") for match in _LINK_PATTERN.finditer(_prose(path)))
    return tuple(target for target in targets if "://" not in target and not target.startswith("mailto:"))


def _prose(path: Path) -> str:
    """Remove fenced and inline code before scanning Markdown links."""
    prose_lines: list[str] = []
    inside_fence = False
    for line in path.read_text(encoding="utf-8").splitlines():
        if _FENCE_PATTERN.fullmatch(line) is not None:
            inside_fence = not inside_fence
        elif not inside_fence:
            prose_lines.append(_INLINE_CODE_PATTERN.sub("", line))
    return "\n".join(prose_lines)


def _heading_anchors(path: Path) -> frozenset[str]:
    """Build the GitHub-style anchors used by the maintained headings."""
    anchors: set[str] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        match = _HEADING_PATTERN.fullmatch(line)
        if match is None:
            continue
        normalized = _ANCHOR_CHARACTERS.sub("", match.group("heading").lower()).replace(" ", "-")
        anchors.add(normalized)
    return frozenset(anchors)


@pytest.mark.parametrize("example", _python_fences(), ids=lambda example: f"{example.path.name}:{example.line_number}")
def test_python_fenced_example_is_syntax_valid(example: _CodeFence) -> None:
    """Compile snippets in an async scope so documented ``await`` remains valid."""
    wrapped = "async def _documented_example() -> None:\n" + indent(example.source or "pass", "    ")
    compile(wrapped, f"{example.path}:{example.line_number}", "exec", dont_inherit=True)


@pytest.mark.parametrize("example_path", _EXAMPLE_MODULES, ids=lambda path: path.name)
def test_example_module_is_syntax_valid(example_path: Path) -> None:
    """Compile every extracted example as a standalone Python module."""
    compile(example_path.read_text(encoding="utf-8"), str(example_path), "exec", dont_inherit=True)


@pytest.mark.asyncio
async def test_inmemory_example_owns_backend_lifecycle_explicitly() -> None:
    """Pin InMemory's omission and the documented drain-before-close order."""
    events: list[str] = []
    backend = _LifecycleProbeBackend(events)
    broker = InMemoryBroker(await_inplace=False).with_result_backend(backend)

    @broker.task(task_name="documentation.lifecycle-probe")
    async def answer() -> int:
        return 42

    try:
        await broker.startup()
        events.append("broker_started")
        assert not backend.started
        await backend.startup()
        await answer.kiq()
    finally:
        try:
            await broker.wait_all()
            events.append("broker_drained")
        finally:
            try:
                await broker.shutdown()
                events.append("broker_stopped")
            finally:
                await backend.shutdown()

    assert events == [
        "broker_started",
        "backend_started",
        "result_stored",
        "broker_drained",
        "broker_stopped",
        "backend_stopped",
    ]


@pytest.mark.parametrize("document", _MARKDOWN_DOCUMENTS, ids=lambda path: path.name)
def test_local_markdown_links_resolve(document: Path) -> None:
    """Require every local prose link and explicit heading fragment to resolve."""
    for target in _local_links(document):
        raw_path, separator, fragment = target.partition("#")
        linked_path = document if not raw_path else (document.parent / raw_path).resolve()
        assert linked_path.exists(), f"broken link in {document}: {target}"
        if separator and fragment:
            assert linked_path.suffix == ".md", f"fragment target is not Markdown in {document}: {target}"
            assert fragment in _heading_anchors(linked_path), f"broken fragment in {document}: {target}"
