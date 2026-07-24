"""Shared UUIDv4 validation for persisted logical identities."""

from typing import Final
from uuid import RFC_4122, UUID


_UUID_VERSION: Final = 4


def require_uuid4(candidate: object, *, field: str) -> UUID:
    """Require a random RFC 4122 UUIDv4 without coercion."""
    if not isinstance(candidate, UUID):
        msg = f"{field} must be a UUID; value must be a random RFC 4122 UUIDv4"
        raise TypeError(msg)
    if candidate.version != _UUID_VERSION or candidate.variant != RFC_4122:
        msg = f"{field} must be a random RFC 4122 UUIDv4"
        raise ValueError(msg)
    return candidate
