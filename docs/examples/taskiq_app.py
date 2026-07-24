"""Syntax-checkable one-shot InMemory flow and schema factory example."""

__all__ = ("add", "broker", "make_result_backend", "result_backend", "run_once")

from datetime import timedelta
import os

from taskiq import InMemoryBroker

from taskiq_clickhouse import ClickHouseResultBackend


_SECURE_CONFIGURATION_ERROR = "CLICKHOUSE_SECURE must be exactly 'true' or 'false'"


def _secure_from_environment() -> bool:
    """Parse one explicit boolean instead of relying on string truthiness."""
    configured = os.environ.get("CLICKHOUSE_SECURE", "true")
    if configured == "true":
        return True
    if configured == "false":
        return False
    raise ValueError(_SECURE_CONFIGURATION_ERROR)


def _port_from_environment() -> int | None:
    """Leave port selection to clickhouse-connect unless explicitly set."""
    configured = os.environ.get("CLICKHOUSE_PORT")
    return None if configured is None else int(configured)


def make_result_backend() -> ClickHouseResultBackend[object]:
    """Return a fresh NEW backend for Taskiq or taskiq-clickhouse-schema."""
    return ClickHouseResultBackend(
        host=os.environ["CLICKHOUSE_HOST"],
        port=_port_from_environment(),
        username=os.environ.get("CLICKHOUSE_USER"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
        database=os.environ["CLICKHOUSE_DATABASE"],
        secure=_secure_from_environment(),
        ca_cert=os.environ.get("CLICKHOUSE_CA_CERT"),
        server_host_name=os.environ.get("CLICKHOUSE_SERVER_HOST_NAME"),
        result_ttl=timedelta(days=1),
        purge_ttl=timedelta(days=7),
        namespace=os.environ.get("TASKIQ_RESULT_NAMESPACE", "production"),
        schema_mode="validate",
    )


result_backend = make_result_backend()
broker = InMemoryBroker(await_inplace=False).with_result_backend(result_backend)


@broker.task(task_name="example.add")
async def add(left: int, right: int) -> int:
    """Return one result that Taskiq persists through the configured backend."""
    return left + right


async def run_once(left: int, right: int) -> int:
    """Run one in-memory task with explicit result-backend ownership."""
    try:
        await broker.startup()
        await result_backend.startup()
        task = await add.kiq(left, right)
        result = await task.wait_result(check_interval=0, timeout=10)
        return result.return_value
    finally:
        # InMemoryBroker does not call its attached result backend.
        try:
            await broker.wait_all()
        finally:
            try:
                await broker.shutdown()
            finally:
                await result_backend.shutdown()
