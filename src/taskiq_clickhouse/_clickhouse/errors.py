"""Stable failures emitted by the concrete ClickHouse adapter."""


class AmbiguousClickHouseError(Exception):
    """Report an I/O failure whose server-side outcome is unknown."""


class DefiniteClickHouseError(Exception):
    """Report a driver failure known not to be an ambiguous transport loss."""
