"""Validate storage policy value objects at the configuration boundary."""

from dataclasses import FrozenInstanceError
from typing import Any, cast

import pytest

from taskiq_clickhouse._datetime64 import MAX_RETENTION_INTERVAL_US
from taskiq_clickhouse._storage_policy import NamespaceKey, RetentionPolicy, StoragePolicy


RESULT_TTL_US = 3_600_000_000
PURGE_TTL_US = 86_400_000_000


def test_storage_policy_is_one_immutable_validated_contract() -> None:
    """Keep namespace and ordered retention coupled after construction."""
    namespace = NamespaceKey("tenant:blue")
    retention = RetentionPolicy(RESULT_TTL_US, PURGE_TTL_US)
    policy = StoragePolicy(namespace, retention)

    assert policy.namespace is namespace
    assert policy.retention is retention
    with pytest.raises(FrozenInstanceError):
        cast("Any", policy).retention = RetentionPolicy(1, 2)


@pytest.mark.parametrize(
    ("candidate", "error_type"),
    [
        (1, TypeError),
        ("", ValueError),
        ("bad namespace", ValueError),
        ("x" * 129, ValueError),
    ],
)
def test_namespace_key_rejects_values_outside_the_persisted_grammar(
    candidate: object,
    error_type: type[Exception],
) -> None:
    """Reject coercion, whitespace and keys wider than the metadata contract."""
    with pytest.raises(error_type, match="namespace must match the storage contract"):
        NamespaceKey(candidate)


@pytest.mark.parametrize(
    ("result_ttl", "purge_ttl", "error_type", "match"),
    [
        (True, PURGE_TTL_US, TypeError, "result_ttl_us must be an integer"),
        (0, PURGE_TTL_US, ValueError, "result_ttl_us must be positive"),
        (RESULT_TTL_US, False, TypeError, "purge_ttl_us must be an integer"),
        (RESULT_TTL_US, 0, ValueError, "purge_ttl_us must be positive"),
        (RESULT_TTL_US, RESULT_TTL_US, ValueError, "must be lower"),
        (PURGE_TTL_US, RESULT_TTL_US, ValueError, "must be lower"),
        (
            MAX_RETENTION_INTERVAL_US + 1,
            MAX_RETENTION_INTERVAL_US + 2,
            ValueError,
            "result_ttl_us must fit the DateTime64 retention range",
        ),
        (
            MAX_RETENTION_INTERVAL_US - 1,
            MAX_RETENTION_INTERVAL_US + 1,
            ValueError,
            "purge_ttl_us must fit the DateTime64 retention range",
        ),
    ],
)
def test_retention_policy_rejects_invalid_ttl_contracts(
    result_ttl: object,
    purge_ttl: object,
    error_type: type[Exception],
    match: str,
) -> None:
    """Reject non-integral, non-positive, unrepresentable and unordered TTLs."""
    with pytest.raises(error_type, match=match):
        RetentionPolicy(result_ttl, purge_ttl)


@pytest.mark.parametrize(
    ("namespace", "retention", "match"),
    [
        (cast("Any", "tenant"), RetentionPolicy(1, 2), "NamespaceKey"),
        (NamespaceKey("tenant"), cast("Any", (1, 2)), "RetentionPolicy"),
    ],
)
def test_storage_policy_rejects_raw_nested_values(
    namespace: NamespaceKey,
    retention: RetentionPolicy,
    match: str,
) -> None:
    """Prevent a repository from receiving partially validated primitives."""
    with pytest.raises(TypeError, match=match):
        StoragePolicy(namespace, retention)
