"""Consumer-owned repository capability used by backend orchestration."""

from typing import Protocol

from taskiq_clickhouse._progress_ports import ProgressReader, ProgressWriter
from taskiq_clickhouse._result_ports import (
    ResultReadinessStore,
    ResultStore,
    ResultWriter,
    StoredResult,
)


class BackendRepository(  # noqa: WPS215 - one aggregate Protocol is the intersection of five narrow use-case ports.
    ResultWriter,
    ResultReadinessStore,
    ResultStore[StoredResult],
    ProgressWriter,
    ProgressReader,
    Protocol,
):
    """Complete storage capability required by the public backend facade."""
