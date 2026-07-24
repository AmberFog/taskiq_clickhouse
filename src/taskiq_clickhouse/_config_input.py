"""Untrusted values accepted by the public backend constructor."""

from dataclasses import dataclass, field
from datetime import timedelta

from taskiq_clickhouse._types import SchemaMode


@dataclass(frozen=True, slots=True, repr=False)
class RawEndpointConfig:
    """Untrusted endpoint and timeout constructor values."""

    host: str
    database: str
    secure: bool
    port: int | None
    connect_timeout: int
    send_receive_timeout: int


@dataclass(frozen=True, slots=True, repr=False)
class RawAuthenticationConfig:
    """Untrusted credentials and TLS constructor values."""

    username: str | None
    password: str = field(repr=False)
    access_token: str | None = field(repr=False)
    ca_cert: str | None
    client_cert: str | None
    client_cert_key: str | None = field(repr=False)
    server_host_name: str | None


@dataclass(frozen=True, slots=True, repr=False)
class RawStorageConfig:
    """Untrusted storage and backend policy constructor values."""

    result_ttl: timedelta
    purge_ttl: timedelta
    namespace: str
    result_table: str
    progress_table: str
    keep_results: bool
    serializer_id: str | None
    schema_mode: SchemaMode


@dataclass(frozen=True, slots=True, repr=False)
class RawBackendConfig:
    """Cohesive untrusted values assembled by the public constructor."""

    endpoint: RawEndpointConfig
    authentication: RawAuthenticationConfig
    storage: RawStorageConfig
