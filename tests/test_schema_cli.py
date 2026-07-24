"""Verify the installed controlled schema-management console command."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import timedelta
import os
from pathlib import Path
import subprocess
import sys
from typing import TYPE_CHECKING, Final, cast
from unittest.mock import patch

import pytest

from taskiq_clickhouse import (
    _cli as cli_module,
)
from taskiq_clickhouse._backend_composition import BackendComponents
from taskiq_clickhouse._backend_runtime import BackendRuntime, RuntimeDependencies
from taskiq_clickhouse._identifiers import Identifier, QualifiedTable
from taskiq_clickhouse._progress_serialization import ProgressCodec
from taskiq_clickhouse._schema_drift import SchemaDriftLocation, SchemaDriftReport
from taskiq_clickhouse._serialization import ResultCodec
import taskiq_clickhouse.backend as backend_module
from taskiq_clickhouse.backend import ClickHouseResultBackend
from taskiq_clickhouse.exceptions import ClickHouseMigrationError, _PhysicalSchemaDriftError


if TYPE_CHECKING:
    from collections.abc import Generator

    from clickhouse_connect.driver.asyncclient import AsyncClient
    from taskiq.abc.serializer import TaskiqSerializer

    from taskiq_clickhouse._config_models import BackendConfig
    from taskiq_clickhouse._storage.repository import StorageRepository
    from taskiq_clickhouse._types import SchemaActor, SchemaMode


_MARKER_ENV: Final = "TASKIQ_CLICKHOUSE_CLI_TEST_MARKER"
_SECRET: Final = "password=cli-secret dsn=https://private.internal payload=task-log"  # noqa: S105
_USAGE_EXIT: Final = 2
NOT_CALLABLE_FACTORY: Final = 42


@dataclass(slots=True)
class _CliRuntime:
    """Subprocess-safe schema-manager collaborator installed by composition."""

    is_new: bool = True
    failure: Exception | None = None

    async def run_schema_manager(
        self,
        *,
        mode: SchemaMode,
        actor: SchemaActor,
    ) -> None:
        """Record exact manager policy and optionally raise one configured failure."""
        await asyncio.to_thread(_write_marker, f"{mode}:{actor.value}")
        if self.failure is not None:
            raise self.failure


@dataclass(slots=True)
class _CliClient:
    """Minimal temporary client owned by the real backend runtime."""

    close_calls: int = 0

    async def close(self) -> None:
        """Observe the runtime-owned cleanup."""
        self.close_calls += 1


class _BackendSubclass(ClickHouseResultBackend[object]):
    pass


class _AsyncCallableFactory:
    async def __call__(self) -> ClickHouseResultBackend[object]:
        """Return a coroutine while hiding it behind a callable object."""
        return _base_backend()


ASYNC_CALLABLE_FACTORY: Final = _AsyncCallableFactory()


class _ExplosiveSignatureFactory:
    @property
    def __signature__(self) -> object:
        """Raise unsafe reflection text before the callable can execute."""
        raise RuntimeError(_SECRET)

    def __call__(self) -> ClickHouseResultBackend[object]:
        """Remain a callable so reflection reaches the hostile property."""
        return _base_backend()


EXPLOSIVE_SIGNATURE_FACTORY: Final = _ExplosiveSignatureFactory()


class _CustomAwaitable:
    """Awaitable non-coroutine returned by an invalid synchronous factory."""

    def __await__(self) -> Generator[None, None, ClickHouseResultBackend[object]]:
        yield from ()
        return _base_backend()


def _base_backend(
    runtime: BackendRuntime | _CliRuntime | None = None,
    *,
    backend_type: type[ClickHouseResultBackend[object]] = ClickHouseResultBackend,
) -> ClickHouseResultBackend[object]:
    selected_runtime = runtime or _CliRuntime()

    def compose(
        config: BackendConfig,
        serializer: TaskiqSerializer,
    ) -> BackendComponents:
        return BackendComponents(
            runtime=cast("BackendRuntime", selected_runtime),
            result_codec=ResultCodec(serializer),
            progress_codec=ProgressCodec(serializer),
            keep_results=config.storage.keep_results,
        )

    with patch.object(backend_module, "compose_backend", compose):
        return backend_type(
            host="localhost",
            database="tasks",
            secure=False,
            result_ttl=timedelta(days=1),
            purge_ttl=timedelta(days=7),
        )


def successful_factory() -> ClickHouseResultBackend[object]:
    """Build a subprocess-safe backend whose barrier succeeds."""
    return _base_backend()


def operation_failure_factory() -> ClickHouseResultBackend[object]:
    """Build a backend whose injected barrier raises unsafe raw text."""
    return _base_backend(_CliRuntime(failure=RuntimeError(_SECRET)))


def public_failure_factory() -> ClickHouseResultBackend[object]:
    """Build a backend whose barrier raises a safe classified package error."""
    error = ClickHouseMigrationError("migration_execute", "database_error")
    return _base_backend(_CliRuntime(failure=error))


def physical_drift_factory() -> ClickHouseResultBackend[object]:
    """Cross the real runtime translation with structured physical drift."""
    runtime = BackendRuntime(
        RuntimeDependencies(
            client_factory=_runtime_client_factory,
            schema_runner=_physical_drift_barrier,
            repository_factory=_unused_repository_factory,
        ),
        schema_mode="validate",
    )
    return _base_backend(runtime)


async def _runtime_client_factory() -> AsyncClient:
    return cast("AsyncClient", _CliClient())


async def _physical_drift_barrier(
    client: AsyncClient,
    *,
    mode: SchemaMode,
    actor: SchemaActor,
) -> None:
    del client
    await asyncio.to_thread(_write_marker, f"{mode}:{actor.value}")
    table = QualifiedTable(Identifier("tasks"), Identifier("taskiq_clickhouse_results"))
    report = SchemaDriftReport(
        mismatch_count=4,
        locations=(
            SchemaDriftLocation(table, "columns[2].type"),
            SchemaDriftLocation(table, "settings.index_granularity"),
            SchemaDriftLocation(table, "auxiliary.constraints"),
            SchemaDriftLocation(table, "unsupported.path"),
        ),
    )
    raise _PhysicalSchemaDriftError(report)


def _unused_repository_factory(client: AsyncClient) -> StorageRepository:
    del client
    message = "schema manager must not create a storage repository"
    raise AssertionError(message)


def configuration_failure_factory() -> ClickHouseResultBackend[object]:
    """Raise unsafe import-time configuration text for CLI redaction."""
    raise RuntimeError(_SECRET)


async def asynchronous_factory() -> ClickHouseResultBackend[object]:
    """Represent a forbidden asynchronous application factory."""
    return _base_backend()


def argument_factory(required: str) -> ClickHouseResultBackend[object]:
    """Represent a forbidden factory that cannot be called without arguments."""
    del required
    return _base_backend()


def wrong_type_factory() -> object:
    """Return a non-backend value."""
    return object()


def subclass_factory() -> ClickHouseResultBackend[object]:
    """Return a supported typed backend extension."""
    return _base_backend(backend_type=_BackendSubclass)


def non_new_factory() -> ClickHouseResultBackend[object]:
    """Return a previously-started backend that is unsafe for CLI ownership."""
    return _base_backend(_CliRuntime(is_new=False))


def malformed_state_factory() -> ClickHouseResultBackend[object]:
    """Return a backend whose lifecycle observation violates its exact contract."""
    runtime = _CliRuntime(is_new=cast("bool", 1))
    return _base_backend(runtime)


def custom_awaitable_factory() -> object:
    """Return a rejected awaitable that has no coroutine close method."""
    return _CustomAwaitable()


def started_coroutine_factory() -> object:
    """Return a rejected started coroutine whose close path fails unsafely."""

    async def rejected_coroutine() -> None:
        try:
            await asyncio.sleep(0)
        finally:
            raise RuntimeError(_SECRET)

    awaitable = rejected_coroutine()
    awaitable.send(None)
    return awaitable


def _run_cli(*arguments: str, marker: Path) -> subprocess.CompletedProcess[str]:
    executable = Path(sys.executable).with_name("taskiq-clickhouse-schema")
    environment = {**os.environ, _MARKER_ENV: str(marker)}
    environment["PYTHONPATH"] = str(Path(__file__).parents[1])
    return subprocess.run(  # noqa: S603 - fixed installed console entry point.
        (str(executable), *arguments),
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
        env=environment,
    )


def _write_marker(value: str) -> None:
    Path(os.environ[_MARKER_ENV]).write_text(value, encoding="utf-8")


@pytest.mark.parametrize(
    ("command", "expected_marker"),
    [("migrate", "migrate:MANAGER"), ("validate", "validate:WORKER")],
)
def test_cli_exact_success_forms(command: str, expected_marker: str, tmp_path: Path) -> None:
    """The two exact public forms return zero and select their frozen policy."""
    marker = tmp_path / "cli-marker"

    result = _run_cli(command, "tests.test_schema_cli:successful_factory", marker=marker)

    assert result.returncode == 0
    assert result.stdout == ""
    assert result.stderr == ""
    assert marker.read_text(encoding="utf-8") == expected_marker


@pytest.mark.parametrize(
    ("command", "expected_marker"),
    [("migrate", "migrate:MANAGER"), ("validate", "validate:WORKER")],
)
def test_cli_main_dispatches_in_process_for_coverage(
    command: str,
    expected_marker: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The synchronous entry point dispatches both manager coroutines."""
    marker = tmp_path / "direct-marker"
    monkeypatch.setenv(_MARKER_ENV, str(marker))

    return_code = cli_module.main([command, "tests.test_schema_cli:successful_factory"])

    assert return_code == 0
    assert marker.read_text(encoding="utf-8") == expected_marker


def test_cli_accepts_typed_backend_subclasses(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The typed CLI contract permits backend extensions in a fresh state."""
    marker = tmp_path / "subclass-marker"
    monkeypatch.setenv(_MARKER_ENV, str(marker))

    return_code = cli_module.main(["validate", "tests.test_schema_cli:subclass_factory"])

    assert return_code == 0
    assert marker.read_text(encoding="utf-8") == "validate:WORKER"


def test_cli_operational_failure_is_exit_one_and_redacted(tmp_path: Path) -> None:
    """Runtime failures return one without exposing raw exception details."""
    marker = tmp_path / "cli-marker"

    result = _run_cli("migrate", "tests.test_schema_cli:operation_failure_factory", marker=marker)

    assert result.returncode == 1
    assert result.stdout == ""
    assert result.stderr == "taskiq-clickhouse-schema: schema operation failed\n"
    assert _SECRET not in result.stderr
    assert "private.internal" not in result.stderr
    assert "Traceback" not in result.stderr
    assert marker.read_text(encoding="utf-8") == "migrate:MANAGER"


def test_cli_preserves_safe_public_operation_and_reason(tmp_path: Path) -> None:
    """Classified package failures retain actionable safe reason codes."""
    marker = tmp_path / "cli-marker"

    result = _run_cli("migrate", "tests.test_schema_cli:public_failure_factory", marker=marker)

    assert result.returncode == 1
    assert result.stdout == ""
    assert result.stderr == ("taskiq-clickhouse-schema: schema operation failed [migration_execute:database_error]\n")
    assert "Traceback" not in result.stderr
    assert marker.read_text(encoding="utf-8") == "migrate:MANAGER"


def test_cli_reports_only_safe_physical_drift_coordinates(tmp_path: Path) -> None:
    """Physical drift identifies locations without exposing catalog values."""
    marker = tmp_path / "cli-marker"

    result = _run_cli("validate", "tests.test_schema_cli:physical_drift_factory", marker=marker)

    assert result.returncode == 1
    assert result.stdout == ""
    assert result.stderr == (
        "taskiq-clickhouse-schema: schema operation failed "
        "[schema_validation:physical_drift]\n"
        "taskiq-clickhouse-schema: physical schema drift mismatches=4 reported=3\n"
        "taskiq-clickhouse-schema: mismatch "
        "table=tasks.taskiq_clickhouse_results path=columns[2].type\n"
        "taskiq-clickhouse-schema: mismatch "
        "table=tasks.taskiq_clickhouse_results path=settings\n"
        "taskiq-clickhouse-schema: mismatch "
        "table=tasks.taskiq_clickhouse_results path=auxiliary.constraints\n"
    )
    assert _SECRET not in result.stderr
    assert "private.internal" not in result.stderr
    assert "Traceback" not in result.stderr
    assert marker.read_text(encoding="utf-8") == "validate:WORKER"


def test_cli_main_returns_one_for_safe_runtime_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The in-process operational branch returns one and writes safe stderr."""
    marker = tmp_path / "direct-marker"
    monkeypatch.setenv(_MARKER_ENV, str(marker))

    return_code = cli_module.main(["migrate", "tests.test_schema_cli:operation_failure_factory"])

    captured = capsys.readouterr()
    assert return_code == 1
    assert captured.out == ""
    assert captured.err == "taskiq-clickhouse-schema: schema operation failed\n"


def test_cli_main_sanitizes_unexpected_ordinary_operation_failure(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Unexpected ordinary operation exceptions use the same stable exit one."""

    async def unexpected_operation(manager: object, command: object) -> None:
        del manager, command
        raise RuntimeError(_SECRET)

    monkeypatch.setattr(cli_module, "_run_manager", unexpected_operation)

    return_code = cli_module.main(["migrate", "tests.test_schema_cli:successful_factory"])

    captured = capsys.readouterr()
    assert return_code == 1
    assert captured.out == ""
    assert captured.err == "taskiq-clickhouse-schema: schema operation failed\n"
    assert _SECRET not in captured.err
    assert _SECRET not in caplog.text


def test_cli_main_rejects_uninspectable_backend_state(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A malformed lifecycle observation is a sanitized usage failure."""
    with pytest.raises(SystemExit) as error_info:
        cli_module.main(["migrate", "tests.test_schema_cli:malformed_state_factory"])

    captured = capsys.readouterr()
    assert error_info.value.code == _USAGE_EXIT
    assert error_info.value.__cause__ is None
    assert captured.out == ""
    assert "backend factory state cannot be inspected" in captured.err
    assert _SECRET not in captured.err


@pytest.mark.parametrize(
    "arguments",
    [
        (),
        ("repair", "tests.test_schema_cli:successful_factory"),
        ("migrate", "invalid"),
        ("migrate", "missing_module:factory"),
        ("migrate", "tests.test_schema_cli:missing_factory"),
        ("migrate", "tests.test_schema_cli:NOT_CALLABLE_FACTORY"),
        ("migrate", "tests.test_schema_cli:configuration_failure_factory"),
        ("migrate", "tests.test_schema_cli:asynchronous_factory"),
        ("migrate", "tests.test_schema_cli:ASYNC_CALLABLE_FACTORY"),
        ("migrate", "tests.test_schema_cli:custom_awaitable_factory"),
        ("migrate", "tests.test_schema_cli:started_coroutine_factory"),
        ("migrate", "tests.test_schema_cli:EXPLOSIVE_SIGNATURE_FACTORY"),
        ("migrate", "tests.test_schema_cli:argument_factory"),
        ("migrate", "tests.test_schema_cli:wrong_type_factory"),
        ("migrate", "tests.test_schema_cli:non_new_factory"),
    ],
)
def test_cli_usage_and_factory_failures_are_exit_two_and_redacted(
    arguments: tuple[str, ...],
    tmp_path: Path,
) -> None:
    """Syntax/import/configuration mistakes return argparse exit code two."""
    result = _run_cli(*arguments, marker=tmp_path / "unused-marker")

    assert result.returncode == _USAGE_EXIT
    assert result.stdout == ""
    assert "usage: taskiq-clickhouse-schema" in result.stderr
    assert _SECRET not in result.stderr
    assert "private.internal" not in result.stderr
    assert "Traceback" not in result.stderr


@pytest.mark.parametrize(
    "arguments",
    [
        [],
        ["repair", "tests.test_schema_cli:successful_factory"],
        ["migrate", "invalid"],
        ["migrate", "missing_module:factory"],
        ["migrate", "tests.test_schema_cli:missing_factory"],
        ["migrate", "tests.test_schema_cli:NOT_CALLABLE_FACTORY"],
        ["migrate", "tests.test_schema_cli:configuration_failure_factory"],
        ["migrate", "tests.test_schema_cli:asynchronous_factory"],
        ["migrate", "tests.test_schema_cli:ASYNC_CALLABLE_FACTORY"],
        ["migrate", "tests.test_schema_cli:custom_awaitable_factory"],
        ["migrate", "tests.test_schema_cli:started_coroutine_factory"],
        ["migrate", "tests.test_schema_cli:EXPLOSIVE_SIGNATURE_FACTORY"],
        ["migrate", "tests.test_schema_cli:argument_factory"],
        ["migrate", "tests.test_schema_cli:wrong_type_factory"],
        ["migrate", "tests.test_schema_cli:non_new_factory"],
    ],
)
def test_cli_main_raises_usage_exit_in_process(arguments: list[str]) -> None:
    """Every syntax, import and factory misuse follows argparse exit two."""
    with pytest.raises(SystemExit) as error_info:
        cli_module.main(arguments)

    assert error_info.value.code == _USAGE_EXIT
    assert error_info.value.__cause__ is None


@pytest.mark.parametrize(
    "specification",
    [
        "missing_module:factory",
        "tests.test_schema_cli:missing_factory",
        "tests.test_schema_cli:configuration_failure_factory",
        "tests.test_schema_cli:EXPLOSIVE_SIGNATURE_FACTORY",
    ],
)
def test_cli_discards_raw_import_and_factory_exception_context(specification: str) -> None:
    """Sanitized usage exits retain no import or configuration exception."""
    with pytest.raises(SystemExit) as error_info:
        cli_module.main(["migrate", specification])

    assert error_info.value.code == _USAGE_EXIT
    assert error_info.value.__cause__ is None
    assert error_info.value.__context__ is None
