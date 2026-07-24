"""Typed input cases for opaque storage projections."""

from dataclasses import dataclass
from typing import Final


_OCTET_COUNT: Final = 256


@dataclass(frozen=True, slots=True)
class OpaqueCase:
    """One exact byte pattern for every opaque storage column."""

    name: str
    result_payload: bytes
    log_payload: bytes
    progress_payload: bytes


OPAQUE_CASES: Final = (
    OpaqueCase("empty", b"", b"", b""),
    OpaqueCase("nul", b"result\x00bytes", b"log\x00bytes", b"progress\x00bytes"),
    OpaqueCase("invalid-utf8", b"\xff\xfe\x80", b"\x80\xff\xfe", b"\xfe\x80\xff"),
    OpaqueCase(
        "all-octets",
        bytes(range(_OCTET_COUNT)),
        bytes(reversed(range(_OCTET_COUNT))),
        bytes(range(_OCTET_COUNT)),
    ),
    OpaqueCase(
        "three-mib",
        b"r" * (3 * 1024 * 1024),
        b"large-result-log",
        b"large-result-progress",
    ),
)
