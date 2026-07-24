"""Console entry point for controlled ClickHouse schema management."""

import argparse
import asyncio
from collections.abc import Callable, Sequence
import importlib
import inspect
import sys
from typing import Any, Final, Literal, cast

from taskiq_clickhouse._cli_reporting import render_operation_failure
from taskiq_clickhouse.backend import ClickHouseResultBackend, _is_new_backend
from taskiq_clickhouse.schema import ClickHouseSchemaManager


_PROGRAM: Final = "taskiq-clickhouse-schema"
_Command = Literal["migrate", "validate"]


def main(argv: Sequence[str] | None = None) -> int:
    """Load one trusted backend factory and run the selected operation."""
    parser = _build_parser()
    arguments = parser.parse_args(argv)
    backend = _load_backend(parser, arguments.backend_factory)
    manager = ClickHouseSchemaManager(backend)
    try:
        asyncio.run(_run_manager(manager, cast("_Command", arguments.command)))
    except Exception as error:  # noqa: BLE001 - renderer exposes only package-owned diagnostics.
        sys.stderr.write(render_operation_failure(error))
        return 1
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog=_PROGRAM, allow_abbrev=False)
    parser.add_argument("command", choices=("migrate", "validate"))
    parser.add_argument("backend_factory", metavar="module:backend_factory")
    return parser


def _load_backend(parser: argparse.ArgumentParser, specification: str) -> ClickHouseResultBackend[Any]:
    factory = _load_factory(parser, specification)
    if not _accepts_zero_arguments(factory):
        parser.error("backend factory must be synchronous and accept zero arguments")
    backend: object = None
    factory_failed = False
    try:
        backend = factory()
    except Exception:  # noqa: BLE001 - configuration details must not reach stderr.
        factory_failed = True
    if factory_failed:
        parser.error("backend factory configuration failed")
    if inspect.isawaitable(backend):
        _close_rejected_awaitable(backend)
        parser.error("backend factory must be synchronous")
    if not isinstance(backend, ClickHouseResultBackend):
        parser.error("backend factory returned an invalid object")
    freshness = _is_new_backend(backend)
    if freshness is None:
        parser.error("backend factory state cannot be inspected")
    if not freshness:
        parser.error("backend factory returned a non-NEW backend")
    return backend


def _load_factory(parser: argparse.ArgumentParser, specification: str) -> Callable[[], object]:
    module_name, separator, attribute_name = specification.partition(":")
    if not separator or not module_name or not attribute_name or ":" in attribute_name:
        parser.error("backend factory must use module:attribute syntax")
    factory: object = None
    import_failed = False
    try:
        factory = getattr(importlib.import_module(module_name), attribute_name)
    except Exception:  # noqa: BLE001 - import-time configuration details must not reach stderr.
        import_failed = True
    if import_failed:
        parser.error("backend factory cannot be imported")
    if not callable(factory):
        parser.error("backend factory is not callable")
    return cast("Callable[[], object]", factory)


def _accepts_zero_arguments(factory: Callable[[], object]) -> bool:
    try:
        inspect.signature(factory).bind()
        is_async = inspect.iscoroutinefunction(factory)
    except Exception:  # noqa: BLE001 - reflection failures are sanitized usage errors.
        return False
    return not is_async


def _close_rejected_awaitable(awaitable: object) -> None:
    if inspect.iscoroutine(awaitable):
        try:
            awaitable.close()
        except Exception:  # noqa: BLE001 - rejected factory cleanup is best effort.
            return


async def _run_manager(manager: ClickHouseSchemaManager, command: _Command) -> None:
    if command == "migrate":
        await manager.migrate()
        return
    await manager.validate()
