"""Side-effect-free validation of backend constructor values."""

from collections.abc import Callable
from datetime import timedelta
from ipaddress import AddressValueError, IPv4Address
import re
from typing import TypeVar

from taskiq_clickhouse._config_input import (
    RawAuthenticationConfig,
    RawBackendConfig,
    RawEndpointConfig,
    RawStorageConfig,
)
from taskiq_clickhouse._config_models import (
    AuthenticationConfig,
    BackendConfig,
    EndpointConfig,
    StorageConfig,
)
from taskiq_clickhouse._datetime64 import MAX_RETENTION_INTERVAL_US
from taskiq_clickhouse._identifiers import METADATA_TABLE_NAME, Identifier
from taskiq_clickhouse._storage_policy import NamespaceKey, RetentionPolicy, StoragePolicy
from taskiq_clickhouse._types import SchemaMode
from taskiq_clickhouse.exceptions import ClickHouseConfigurationError


_CapturedT = TypeVar("_CapturedT")
_INVALID_HOST = "invalid_host"
_IPV6_HOST = "literal_ipv6_or_port_in_host"
_TABLE_CONFLICT = "table_name_conflict"
_INVALID_NAMESPACE = "invalid_namespace"
_INVALID_PORT = "invalid_port"
_INVALID_SCHEMA_MODE = "invalid_schema_mode"
_TTL_ORDER = "ttl_order"
_TTL_RANGE = "ttl_range"
_INVALID_TOKEN_AUTH = "invalid_token_auth"  # noqa: S105 - safe reason code.
_INVALID_MTLS_AUTH = "invalid_mtls_auth"
_TLS_REQUIRES_SECURE = "tls_option_requires_secure"
_KEY_WITHOUT_CERT = "client_cert_key_without_cert"


class _TextRules:
    """Validate textual values at the public constructor boundary."""

    _host_forbidden_pattern = re.compile(r"[\s/@?#\[\]]")
    _dns_label_pattern = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\Z")
    _dotted_numeric_pattern = re.compile(r"(?:[0-9]+\.)+[0-9]+\Z")
    _max_dns_host_length = 253

    def host(self, candidate: object) -> str:
        """Validate one IPv4 or DNS host without a URL or port."""
        host = self.string(candidate, field_name="host")
        invalid_text = (
            not host,
            host != host.strip(),
            "://" in host,
            self._host_forbidden_pattern.search(host) is not None,
        )
        if any(invalid_text):
            raise _error(_INVALID_HOST)
        if ":" in host:
            raise _error(_IPV6_HOST)
        canonical_dns = host.removesuffix(".")
        invalid_ipv4 = self._invalid_ipv4(canonical_dns)
        invalid_label = None in map(
            self._dns_label_pattern.fullmatch,
            canonical_dns.split("."),
        )
        if invalid_ipv4 or len(canonical_dns) > self._max_dns_host_length or invalid_label:
            raise _error(_INVALID_HOST)
        return host

    def identifier(self, candidate: object, *, field_name: str) -> str:
        """Validate one database or table identifier component."""
        identifier = self.string(candidate, field_name=field_name)
        validated = _capture_value(lambda: Identifier(identifier))
        if validated is None:
            raise _field_error(field_name)
        return validated.value

    def distinct_tables(self, result_table: str, progress_table: str) -> None:
        """Keep result, progress and permanent metadata tables distinct."""
        configured = (result_table, progress_table, METADATA_TABLE_NAME)
        if len(configured) != len(set(configured)):
            raise _error(_TABLE_CONFLICT)

    def namespace(self, candidate: object) -> NamespaceKey:
        """Validate a namespace storage key."""
        namespace = _capture_value(lambda: NamespaceKey(candidate))
        if namespace is None:
            raise _error(_INVALID_NAMESPACE)
        return namespace

    def string(self, candidate: object, *, field_name: str) -> str:
        """Require an exact string and translate its error to a field code."""
        if type(candidate) is not str:  # noqa: WPS516 - reject coercible values at the trust boundary.
            raise _field_error(field_name)
        return candidate

    def optional_string(
        self,
        candidate: object,
        *,
        field_name: str,
        allow_empty: bool = False,
    ) -> str | None:
        """Validate an optional string and its empty-value policy."""
        if candidate is None:
            return None
        validated = self.string(candidate, field_name=field_name)
        if not validated and not allow_empty:
            raise _field_error(field_name, prefix="empty")
        return validated

    def _invalid_ipv4(self, host: str) -> bool:
        if self._dotted_numeric_pattern.fullmatch(host) is None:
            return False
        try:
            IPv4Address(host)
        except AddressValueError:
            return True
        return False


class _ScalarRules:
    """Validate exact scalar values and finite retention durations."""

    _seconds_per_day = 86_400
    _microseconds_per_second = 1_000_000
    _min_port = 1
    _max_port = 65_535

    def port(self, candidate: object) -> int | None:
        """Validate an optional HTTP port."""
        if candidate is None:
            return None
        port = self.integer(candidate, field_name="port")
        if not self._min_port <= port <= self._max_port:
            raise _error(_INVALID_PORT)
        return port

    def positive_integer(self, candidate: object, *, field_name: str) -> int:
        """Validate a positive exact integer."""
        integer = self.integer(candidate, field_name=field_name)
        if integer <= 0:
            raise _field_error(field_name)
        return integer

    def integer(self, candidate: object, *, field_name: str) -> int:
        """Reject booleans and non-integers."""
        if type(candidate) is not int:  # noqa: WPS516 - booleans are not numeric policies.
            raise _field_error(field_name)
        return candidate

    def boolean(self, candidate: object, *, field_name: str) -> bool:
        """Require a real bool rather than a truthy integer."""
        if type(candidate) is not bool:  # noqa: WPS516 - truthy integers are invalid here.
            raise _field_error(field_name)
        return candidate

    def schema_mode(self, candidate: object) -> SchemaMode:
        """Validate the exact public schema-mode literals."""
        exact_string = type(candidate) is str  # noqa: WPS516 - public literals require exact strings.
        if not exact_string or candidate not in {"migrate", "validate"}:
            raise _error(_INVALID_SCHEMA_MODE)
        return candidate

    def retention(self, result_ttl: object, purge_ttl: object) -> RetentionPolicy:
        """Convert finite ordered durations to exact microseconds."""
        result_us = self._timedelta_microseconds(result_ttl, field_name="result_ttl")
        purge_us = self._timedelta_microseconds(purge_ttl, field_name="purge_ttl")
        if result_us > MAX_RETENTION_INTERVAL_US or purge_us > MAX_RETENTION_INTERVAL_US:
            raise _error(_TTL_RANGE)
        retention = _capture_value(lambda: RetentionPolicy(result_us, purge_us))
        if retention is None:
            raise _error(_TTL_ORDER)
        return retention

    def _timedelta_microseconds(self, candidate: object, *, field_name: str) -> int:
        if type(candidate) is not timedelta:  # noqa: WPS516 - require a real duration.
            raise _field_error(field_name)
        seconds = (candidate.days * self._seconds_per_day) + candidate.seconds
        microseconds = seconds * self._microseconds_per_second + candidate.microseconds
        if microseconds <= 0:
            raise _field_error(field_name)
        return microseconds


class _AuthenticationRules:
    """Validate exclusive basic, token and mutual-TLS authentication."""

    def __init__(self, text: _TextRules) -> None:
        self._text = text

    def validate(
        self,
        endpoint: EndpointConfig,
        raw: RawAuthenticationConfig,
    ) -> AuthenticationConfig:
        """Return one normalized authentication policy."""
        username = self._text.optional_string(raw.username, field_name="username", allow_empty=True)
        authentication = AuthenticationConfig(
            username=username or None,
            password=self._text.string(raw.password, field_name="password"),
            access_token=self._text.optional_string(raw.access_token, field_name="access_token"),
            ca_cert=self._text.optional_string(raw.ca_cert, field_name="ca_cert"),
            client_cert=self._text.optional_string(raw.client_cert, field_name="client_cert"),
            client_cert_key=self._text.optional_string(raw.client_cert_key, field_name="client_cert_key"),
            server_host_name=self._text.optional_string(raw.server_host_name, field_name="server_host_name"),
        )
        self._validate_mode(secure=endpoint.secure, authentication=authentication)
        return authentication

    def _validate_mode(
        self,
        *,
        secure: bool,
        authentication: AuthenticationConfig,
    ) -> None:
        if authentication.access_token is not None:
            self._token(secure=secure, authentication=authentication)
        elif authentication.client_cert is not None:
            self._mtls(secure=secure, authentication=authentication)
        self._tls_options(secure=secure, authentication=authentication)

    def _token(
        self,
        *,
        secure: bool,
        authentication: AuthenticationConfig,
    ) -> None:
        invalid_identity = authentication.username is not None or bool(authentication.password)
        invalid_auth = not secure or invalid_identity or authentication.client_cert is not None
        if invalid_auth:
            raise _error(_INVALID_TOKEN_AUTH)

    def _mtls(
        self,
        *,
        secure: bool,
        authentication: AuthenticationConfig,
    ) -> None:
        if not secure or not authentication.username or authentication.password:
            raise _error(_INVALID_MTLS_AUTH)

    def _tls_options(
        self,
        *,
        secure: bool,
        authentication: AuthenticationConfig,
    ) -> None:
        tls_options = (
            authentication.ca_cert,
            authentication.client_cert,
            authentication.client_cert_key,
            authentication.server_host_name,
        )
        if not secure and any(option is not None for option in tls_options):
            raise _error(_TLS_REQUIRES_SECURE)
        if authentication.client_cert_key is not None and authentication.client_cert is None:
            raise _error(_KEY_WITHOUT_CERT)


class ConfigurationValidator:
    """Compose independent input policies into one immutable configuration."""

    def __init__(self) -> None:
        self._text = _TextRules()
        self._scalar = _ScalarRules()
        self._authentication = _AuthenticationRules(self._text)

    def validate(
        self,
        raw: RawBackendConfig,
        *,
        serializer_id: str,
    ) -> BackendConfig:
        """Validate every constructor axis without creating resources."""
        endpoint = self._endpoint(raw.endpoint)
        return BackendConfig(
            endpoint=endpoint,
            authentication=self._authentication.validate(endpoint, raw.authentication),
            storage=self._storage(raw.storage, serializer_id=serializer_id),
        )

    def _endpoint(self, raw: RawEndpointConfig) -> EndpointConfig:
        return EndpointConfig(
            host=self._text.host(raw.host),
            database=self._text.identifier(raw.database, field_name="database"),
            secure=self._scalar.boolean(raw.secure, field_name="secure"),
            port=self._scalar.port(raw.port),
            connect_timeout=self._scalar.positive_integer(raw.connect_timeout, field_name="connect_timeout"),
            send_receive_timeout=self._scalar.positive_integer(
                raw.send_receive_timeout,
                field_name="send_receive_timeout",
            ),
        )

    def _storage(
        self,
        raw: RawStorageConfig,
        *,
        serializer_id: str,
    ) -> StorageConfig:
        result_table = self._text.identifier(raw.result_table, field_name="result_table")
        progress_table = self._text.identifier(raw.progress_table, field_name="progress_table")
        self._text.distinct_tables(result_table, progress_table)
        return StorageConfig(
            policy=StoragePolicy(
                namespace=self._text.namespace(raw.namespace),
                retention=self._scalar.retention(raw.result_ttl, raw.purge_ttl),
            ),
            result_table=result_table,
            progress_table=progress_table,
            keep_results=self._scalar.boolean(raw.keep_results, field_name="keep_results"),
            serializer_id=serializer_id,
            schema_mode=self._scalar.schema_mode(raw.schema_mode),
        )


CONFIGURATION_VALIDATOR = ConfigurationValidator()


def _capture_value(factory: Callable[[], _CapturedT]) -> _CapturedT | None:
    try:
        return factory()
    except (TypeError, ValueError):
        return None


def _field_error(field_name: str, *, prefix: str = "invalid") -> ClickHouseConfigurationError:
    reason = f"{prefix}_{field_name.replace('-', '_')}"
    return _error(reason)


def _error(reason: str) -> ClickHouseConfigurationError:
    return ClickHouseConfigurationError("configuration", reason)
