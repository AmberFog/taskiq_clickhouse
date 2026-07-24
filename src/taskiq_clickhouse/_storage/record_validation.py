"""Shared constructor guards for native storage record values."""

from taskiq_clickhouse._storage_policy import NamespaceKey


def require_namespace(candidate: object) -> None:
    """Apply the canonical namespace-key validation contract."""
    NamespaceKey(candidate)


def require_text(candidate: object, *, field: str) -> str:
    """Require a native text value."""
    if not isinstance(candidate, str):
        msg = f"{field} must be a string"
        raise TypeError(msg)
    return candidate


def require_bytes(candidate: object, *, field: str) -> bytes:
    """Require exact opaque bytes and reject coercible subclasses."""
    if type(candidate) is not bytes:  # noqa: WPS516 - exact native bytes are the storage contract.
        msg = f"{field} must be bytes"
        raise TypeError(msg)
    return candidate


def require_instance(candidate: object, item_type: type[object], *, field: str) -> None:
    """Require one already-validated package domain value."""
    if not isinstance(candidate, item_type):
        msg = f"{field} must be a {item_type.__name__}"
        raise TypeError(msg)
