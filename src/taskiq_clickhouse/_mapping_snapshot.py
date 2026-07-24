"""Exact mapping snapshots shared by persistence serialization boundaries."""

from collections.abc import Mapping
from typing import cast


def materialize_exact_mapping(
    candidate: object,
    field_names: tuple[str, ...],
    *,
    require_dict: bool,
) -> dict[str, object]:
    """Copy a supported mapping only when it has the exact persisted fields."""
    valid_container = isinstance(candidate, Mapping)
    if require_dict:
        valid_container = type(candidate) is dict  # noqa: WPS516 - require a plain Pydantic dump.
    if not valid_container:
        msg = "candidate must be a supported mapping"
        raise TypeError(msg)
    copied = dict(cast("Mapping[object, object]", candidate))
    if frozenset(copied) != frozenset(field_names):
        msg = "mapping fields do not match the persisted contract"
        raise ValueError(msg)
    return {field_name: copied[field_name] for field_name in field_names}
