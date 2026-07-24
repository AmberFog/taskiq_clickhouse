"""Factories for valid backend objects and validated backend configuration."""

from datetime import timedelta

from factory.base import Factory
from factory.declarations import LazyAttributeSequence

from taskiq_clickhouse._config_models import (
    AuthenticationConfig,
    BackendConfig,
    EndpointConfig,
    StorageConfig,
)
from taskiq_clickhouse._storage_policy import NamespaceKey, RetentionPolicy, StoragePolicy
from taskiq_clickhouse.backend import ClickHouseResultBackend
from tests.factories import incidental


class ClickHouseResultBackendFactory(Factory[ClickHouseResultBackend[object]]):
    """Build a valid backend while exposing only scenario-specific overrides."""

    class Meta:
        """Bind this factory to the exact public backend type."""

        model = ClickHouseResultBackend

    host = "localhost"
    database = LazyAttributeSequence(  # type: ignore[no-untyped-call]
        lambda _backend, sequence: incidental.identifier("tasks", sequence).replace("-", "_"),
    )
    secure = False
    result_ttl = timedelta(days=1)
    purge_ttl = timedelta(days=7)


class BackendConfigFactory(Factory[BackendConfig]):
    """Build the repeated validated configuration graph used by adapter tests."""

    class Meta:
        """Bind this factory to the immutable validated config type."""

        model = BackendConfig

    endpoint = EndpointConfig(
        host="localhost",
        database="tasks",
        secure=False,
        port=None,
        connect_timeout=10,
        send_receive_timeout=300,
    )
    authentication = AuthenticationConfig(
        username=None,
        password="",
        access_token=None,
        ca_cert=None,
        client_cert=None,
        client_cert_key=None,
        server_host_name=None,
    )
    storage = StorageConfig(
        policy=StoragePolicy(
            NamespaceKey("default"),
            RetentionPolicy(86_400_000_000, 604_800_000_000),
        ),
        result_table="taskiq_clickhouse_results",
        progress_table="taskiq_clickhouse_progress",
        keep_results=True,
        serializer_id="taskiq-json-v1",
        schema_mode="migrate",
    )
