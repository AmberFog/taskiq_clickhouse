"""Owned local TCP endpoints for deterministic startup failures."""

from __future__ import annotations

from contextlib import contextmanager
import socket
from typing import TYPE_CHECKING, Literal


if TYPE_CHECKING:
    from collections.abc import Iterator


NetworkMode = Literal["refused", "timeout"]


@contextmanager
def reserved_endpoint(mode: NetworkMode) -> Iterator[int]:
    """Keep an ephemeral port reserved as refused or non-responsive."""
    endpoint = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    endpoint.bind(("127.0.0.1", 0))
    if mode == "timeout":
        endpoint.listen(16)
    try:
        yield int(endpoint.getsockname()[1])
    finally:
        endpoint.close()
