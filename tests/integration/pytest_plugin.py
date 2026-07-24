"""Strict selection semantics for required ClickHouse test runs."""

from collections.abc import Generator
from typing import Final

import pytest


CLICKHOUSE_MARKER: Final = "clickhouse"
REQUIRED_OPTION: Final = "--clickhouse-required"
REQUIRED_DEST: Final = "clickhouse_required"


def pytest_addoption(parser: pytest.Parser) -> None:
    """Add an explicit opt-in for fail-closed ClickHouse execution."""
    group = parser.getgroup("taskiq-clickhouse")
    group.addoption(
        REQUIRED_OPTION,
        action="store_true",
        dest=REQUIRED_DEST,
        default=False,
        help="require a healthy ClickHouse service and prohibit skips",
    )


def pytest_configure(config: pytest.Config) -> None:
    """Register the project-owned marker before strict collection."""
    config.addinivalue_line(
        "markers",
        f"{CLICKHOUSE_MARKER}: requires the real ClickHouse integration service",
    )


@pytest.hookimpl(trylast=True)
def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Reject implicit service runs and malformed required selections."""
    clickhouse_items = [item for item in items if _is_clickhouse_item(item)]
    required = _is_required(config)
    if clickhouse_items and not required:
        msg = f"ClickHouse tests require {REQUIRED_OPTION}"
        raise pytest.UsageError(msg)
    if not required:
        return
    if not clickhouse_items:
        msg = "required ClickHouse run selected no ClickHouse tests"
        raise pytest.UsageError(msg)
    if len(clickhouse_items) != len(items):
        msg = "required ClickHouse run may select only ClickHouse tests"
        raise pytest.UsageError(msg)


@pytest.hookimpl(wrapper=True, trylast=True)
def pytest_runtest_makereport(
    item: pytest.Item,
    call: pytest.CallInfo[None],
) -> Generator[None, pytest.TestReport, pytest.TestReport]:
    """Turn every skip in required mode into an ordinary test failure."""
    del call
    report = yield
    if _is_required(item.config) and _is_clickhouse_item(item) and report.skipped:
        report.outcome = "failed"
        report.longrepr = "ClickHouse tests may not skip in required mode"
    return report


def _is_required(config: pytest.Config) -> bool:
    return bool(config.getoption(REQUIRED_DEST))


def _is_clickhouse_item(item: pytest.Item) -> bool:
    return item.get_closest_marker(CLICKHOUSE_MARKER) is not None
