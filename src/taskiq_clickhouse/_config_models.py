"""Validated immutable backend policies."""

from dataclasses import dataclass, field

from taskiq_clickhouse._storage_policy import StoragePolicy
from taskiq_clickhouse._types import SchemaMode


@dataclass(frozen=True, slots=True, repr=False)
class EndpointConfig:
    """Validated connection endpoint and bounded driver timeouts."""

    host: str
    database: str
    secure: bool
    port: int | None
    connect_timeout: int
    send_receive_timeout: int


@dataclass(frozen=True, slots=True, repr=False)
class AuthenticationConfig:
    """Validated credentials and TLS options with secret-safe repr."""

    username: str | None
    password: str = field(repr=False)
    access_token: str | None = field(repr=False)
    ca_cert: str | None
    client_cert: str | None
    client_cert_key: str | None = field(repr=False)
    server_host_name: str | None


@dataclass(frozen=True, slots=True, repr=False)
class StorageConfig:
    """Validated namespace, tables, retention and backend policy."""

    policy: StoragePolicy
    result_table: str
    progress_table: str
    keep_results: bool
    serializer_id: str
    schema_mode: SchemaMode


@dataclass(frozen=True, slots=True, repr=False)
class BackendConfig:
    """Validated configuration grouped by independent change axis."""

    endpoint: EndpointConfig
    authentication: AuthenticationConfig
    storage: StorageConfig
