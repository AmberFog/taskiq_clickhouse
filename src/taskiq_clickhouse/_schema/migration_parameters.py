"""Immutable text bindings covered by migration checksums."""

from collections.abc import Mapping
from types import MappingProxyType
from typing import cast


def empty_query_parameters() -> Mapping[str, str]:
    """Return an immutable empty binding set for synthetic migrations."""
    return MappingProxyType({})


def freeze_query_parameters(candidate: object) -> Mapping[str, str]:
    """Copy, validate and deterministically order persistent DDL bindings."""
    if not isinstance(candidate, Mapping):
        msg = "migration query parameters must be a mapping"
        raise TypeError(msg)
    copied = dict(cast("Mapping[object, object]", candidate))
    for parameter_name, parameter_value in copied.items():
        _validate_pair(parameter_name, parameter_value)
    validated = cast("dict[str, str]", copied)
    return MappingProxyType(dict(sorted(validated.items())))


def _validate_pair(parameter_name: object, parameter_value: object) -> None:
    validated_name = _exact_text(parameter_name)
    validated_value = _exact_text(parameter_value)
    if not validated_name or not validated_value:
        msg = "migration query parameters must contain non-empty NUL-free text"
        raise ValueError(msg)
    if "\x00" in validated_name or "\x00" in validated_value:
        msg = "migration query parameters must contain non-empty NUL-free text"
        raise ValueError(msg)


def _exact_text(candidate: object) -> str:
    if type(candidate) is not str:  # noqa: WPS516 - persisted bindings reject subclasses.
        msg = "migration query parameters must contain string pairs"
        raise TypeError(msg)
    return candidate
