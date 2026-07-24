"""Exact ownership of shielded async-client factory and close tasks."""

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Generic, TypeVar

from clickhouse_connect.driver.asyncclient import AsyncClient

from taskiq_clickhouse._lifecycle import SchemaOperation


_TaskResultT = TypeVar("_TaskResultT")
_SCHEMA_MANAGER_OPERATION = "schema_manager"

ClientFactory = Callable[[], Awaitable[AsyncClient]]


async def _invoke_factory(client_factory: ClientFactory) -> AsyncClient:
    return await client_factory()


async def _invoke_close(client: AsyncClient) -> None:
    await client.close()


def _caller_is_cancelling() -> bool:
    caller = asyncio.current_task()
    return caller is not None and caller.cancelling() > 0


@dataclass(frozen=True, slots=True)
class _OwnedTask(Generic[_TaskResultT]):
    """Retain one shielded task until its exact terminal outcome."""

    task: asyncio.Task[_TaskResultT]

    async def wait(self) -> _TaskResultT:
        """Shield the owned operation from its caller's cancellation."""
        return await asyncio.shield(self.task)

    async def drain(self) -> None:
        """Keep ownership through cancellation and surface terminal fatal signals."""
        while not self.task.done():
            try:
                await asyncio.shield(self.task)
            except asyncio.CancelledError:
                continue
            except Exception:  # noqa: BLE001 - ordinary cleanup failure is subordinate.
                break
        self._propagate_terminal_fatal()

    def successful_result(self) -> _TaskResultT | None:
        """Return a successful terminal value without surfacing cleanup errors."""
        if self.task.cancelled() or self.task.exception() is not None:
            return None
        return self.task.result()

    def propagate_dependency_cancellation(self) -> None:
        """Recover the exact cancellation object hidden by asyncio.shield."""
        self.task.result()

    def _propagate_terminal_fatal(self) -> None:
        if self.task.cancelled():
            return
        terminal_error = self.task.exception()
        if terminal_error is not None and not isinstance(terminal_error, Exception):
            raise terminal_error


@dataclass(frozen=True, slots=True)
class OwnedClient:
    """Client setup and cleanup bound to one safe public operation."""

    operation: str = _SCHEMA_MANAGER_OPERATION

    async def open(self, client_factory: ClientFactory) -> AsyncClient:
        """Create one client and retain ownership through caller cancellation."""
        factory_task = _OwnedTask(asyncio.create_task(_invoke_factory(client_factory)))
        try:
            return await factory_task.wait()
        except asyncio.CancelledError:
            if not _caller_is_cancelling():
                factory_task.propagate_dependency_cancellation()
            await factory_task.drain()
            await self._close_late_factory_result(factory_task.successful_result())
            raise
        except Exception:  # noqa: BLE001 - raw driver details are deliberately discarded.
            failure = SchemaOperation(self.operation).backend_error("client_create_failed")
        raise failure from None

    async def close(self, client: AsyncClient) -> None:
        """Run exactly one close call and retain ownership through cancellation."""
        close_task = _OwnedTask(asyncio.create_task(_invoke_close(client)))
        try:
            await close_task.wait()
        except asyncio.CancelledError:
            if not _caller_is_cancelling():
                close_task.propagate_dependency_cancellation()
            await close_task.drain()
            raise
        except Exception:  # noqa: BLE001 - raw driver details are deliberately discarded.
            failure = SchemaOperation(self.operation).backend_error("client_close_failed")
        else:
            return
        raise failure from None

    async def close_after_cancel(self, client: AsyncClient) -> None:
        """Finish cleanup while preserving the caller's cancellation."""
        close_task = _OwnedTask(asyncio.create_task(_invoke_close(client)))
        await close_task.drain()

    async def close_after_failure(self, client: AsyncClient) -> None:
        """Suppress ordinary cleanup failure without swallowing fatal signals."""
        try:
            await self.close(client)
        except Exception:  # noqa: BLE001 - operation failure remains authoritative.
            return

    async def _close_late_factory_result(self, client: AsyncClient | None) -> None:
        if client is not None:
            await self.close_after_cancel(client)
