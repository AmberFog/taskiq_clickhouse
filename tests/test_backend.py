"""Verify the frozen backend constructor and NEW-state shell."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterator, Mapping
from dataclasses import dataclass, field
from datetime import timedelta
from importlib.metadata import version as distribution_version
import inspect
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, Final, cast

import clickhouse_connect
import pytest
from taskiq.abc.serializer import TaskiqSerializer
from taskiq.depends.progress_tracker import TaskProgress
from taskiq.result import TaskiqResult
from taskiq.serializers.json_serializer import JSONSerializer
from taskiq.serializers.pickle import PickleSerializer

from taskiq_clickhouse import (
    _backend_composition as backend_composition,
    _lifecycle as lifecycle_module,
)
from taskiq_clickhouse._backend_composition import BackendComponents
from taskiq_clickhouse._config_models import AuthenticationConfig
from taskiq_clickhouse._identifiers import METADATA_TABLE_NAME
from taskiq_clickhouse._progress_serialization import ProgressCodec
from taskiq_clickhouse._serialization import ResultCodec
import taskiq_clickhouse.backend as backend_module
from taskiq_clickhouse.backend import ClickHouseResultBackend, _is_new_backend
from taskiq_clickhouse.exceptions import ClickHouseConfigurationError, ClickHouseLifecycleError
from tests.factories.backend import BackendConfigFactory, ClickHouseResultBackendFactory
from tests.result_contract.assertions import assert_production_traceback_excludes


if TYPE_CHECKING:
    from clickhouse_connect.driver.asyncclient import AsyncClient

    from taskiq_clickhouse._backend_runtime import BackendRuntime
    from taskiq_clickhouse._config_models import BackendConfig
    from taskiq_clickhouse._storage.repository import StorageRepository
    from taskiq_clickhouse._storage.result_records import ResultRead


_PASSWORD: Final = "password"  # noqa: S105 - inert unit-test credential.
_ACCESS_TOKEN: Final = "token"  # noqa: S105 - inert unit-test credential.
_PRIVATE_PASSWORD: Final = "PRIVATE_CONSTRUCTOR_PASSWORD"  # noqa: S105 - redaction sentinel.
_PRIVATE_TOKEN: Final = "PRIVATE_CONSTRUCTOR_TOKEN"  # noqa: S105 - redaction sentinel.


class _CustomSerializer(TaskiqSerializer):
    def dumpb(self, value: Any) -> bytes:  # noqa: ANN401 - inherited serializer contract.
        del value
        return b"custom"

    def loadb(self, value: bytes) -> Any:  # noqa: ANN401 - inherited serializer contract.
        return value


class _JsonSubclass(JSONSerializer):
    pass


class _HostileSerializerClassLookup(TaskiqSerializer):
    """Remain a valid custom serializer while rejecting instance class lookup."""

    def __getattribute__(self, attribute_name: str) -> object:
        if attribute_name == "__class__":
            message = "password=serializer-class-secret"  # pragma: allowlist secret
            raise RuntimeError(message)
        return super().__getattribute__(attribute_name)

    def dumpb(self, value: Any) -> bytes:  # noqa: ANN401 - inherited serializer contract.
        del value
        return b"custom"

    def loadb(self, value: bytes) -> Any:  # noqa: ANN401 - inherited serializer contract.
        return value


class _SerializerIdSubclass(str):
    """Reject a coercible persistent identity with custom comparison behavior."""

    __slots__ = ()


class _HostileSerializerId:
    """Raise secret-bearing text if constructor validation invokes equality."""

    __hash__ = object.__hash__

    def __eq__(self, candidate: object) -> bool:
        del candidate
        message = "password=serializer-id-secret"  # pragma: allowlist secret
        raise RuntimeError(message)


class _HostileFreshness:
    """Raise if an exact lifecycle observation consults a forged class hook."""

    def __getattribute__(self, attribute_name: str) -> object:
        if attribute_name == "__class__":
            message = "private-freshness-detail"
            raise RuntimeError(message)
        return super().__getattribute__(attribute_name)


class _ExplodingModelFields(Mapping[str, object]):
    """Expose dependency metadata that fails while producing its field snapshot."""

    def __getitem__(self, field_name: str) -> object:
        raise KeyError(field_name)

    def __iter__(self) -> Iterator[str]:
        message = "private-model-fields-detail"
        raise RuntimeError(message)

    def __len__(self) -> int:
        return 1


class _ExplodingAnnotation:
    """Raise from an equality hook on dependency-owned annotation metadata."""

    __hash__ = object.__hash__

    def __eq__(self, candidate: object) -> bool:
        del candidate
        message = "private-annotation-detail"
        raise RuntimeError(message)


class _NonBooleanAnnotation:
    """Return a non-boolean equality result from malformed dependency metadata."""

    __hash__ = object.__hash__

    def __eq__(self, candidate: object) -> bool:
        del candidate
        return cast("bool", object())


class _FakeClient:
    async def close(self) -> None:
        return None


@dataclass(slots=True)
class _CompositionClient:
    """Minimal driver-shaped client for the real package composition root."""

    close_calls: int = 0
    query_calls: int = 0

    async def query(self, *_args: object, **_kwargs: object) -> object:
        """Return an empty result projection through the concrete adapter."""
        self.query_calls += 1
        return SimpleNamespace(result_rows=())

    async def close(self) -> None:
        """Record runtime ownership cleanup."""
        self.close_calls += 1


class _NewRuntime:
    """Minimal runtime observation returned by the composition test seam."""

    @property
    def is_new(self) -> bool:
        """Report the constructor's expected initial lifecycle state."""
        return True


@dataclass(frozen=True, slots=True)
class _MalformedFreshnessRuntime:
    """Expose one invalid composition outcome at the CLI freshness seam."""

    outcome: object

    @property
    def is_new(self) -> bool:
        """Return a forged value or raise one accidental composition failure."""
        if type(self.outcome) is RuntimeError:  # noqa: WPS516 - test only the exact injected failure.
            raise self.outcome
        return cast("bool", self.outcome)


@dataclass(slots=True)
class _CompositionCapture:
    """Capture constructor output at the package composition boundary."""

    configs: list[BackendConfig] = field(default_factory=list)
    serializers: list[TaskiqSerializer] = field(default_factory=list)

    def __call__(
        self,
        config: BackendConfig,
        serializer: TaskiqSerializer,
    ) -> BackendComponents:
        """Record immutable policies and return inert facade collaborators."""
        self.configs.append(config)
        self.serializers.append(serializer)
        return BackendComponents(
            runtime=cast("BackendRuntime", _NewRuntime()),
            result_codec=ResultCodec(serializer),
            progress_codec=ProgressCodec(serializer),
            keep_results=config.storage.keep_results,
        )


@dataclass(frozen=True, slots=True)
class _SelectedResult:
    """Only the selected payload projection consumed by result orchestration."""

    result_payload: bytes = b"stored-result"
    log_payload: bytes | None = b"stored-log"


@dataclass(slots=True)
class _ResultStoreProbe:
    """Record the one storage projection selected by the public facade."""

    selected: _SelectedResult = field(default_factory=_SelectedResult)
    calls: list[tuple[str, str]] = field(default_factory=list)

    async def read_result_with_log(self, task_id: str) -> ResultRead | None:
        """Return the configured result with its independently stored log."""
        self.calls.append(("read-with-log", task_id))
        return cast("ResultRead", self.selected)


@dataclass(slots=True)
class _ResultCodecProbe:
    """Record decoding selected by the public result facade."""

    decoded: TaskiqResult[Any]
    calls: list[tuple[bytes, bytes | None]] = field(default_factory=list)

    async def decode(
        self,
        result_payload: bytes,
        log_payload: bytes | None,
    ) -> TaskiqResult[Any]:
        """Return one generic Taskiq model after recording exact payloads."""
        self.calls.append((result_payload, log_payload))
        return self.decoded


@dataclass(frozen=True, slots=True)
class _ReadyRuntime:
    """Expose one READY repository through the runtime's public capability."""

    store: _ResultStoreProbe

    def repository(self) -> StorageRepository:
        """Return the result-store capability retained by composition."""
        return cast("StorageRepository", self.store)


def _install_composition_capture(monkeypatch: pytest.MonkeyPatch) -> _CompositionCapture:
    capture = _CompositionCapture()
    monkeypatch.setattr(backend_module, "compose_backend", capture)
    return capture


def test_constructor_is_exact_and_side_effect_free(monkeypatch: pytest.MonkeyPatch) -> None:
    """Construction validates only local values and creates no client."""
    calls = 0

    async def forbidden_client_factory(**kwargs: object) -> AsyncClient:
        """Fail if construction performs the deferred call."""
        del kwargs
        nonlocal calls
        calls += 1
        raise AssertionError

    monkeypatch.setattr(clickhouse_connect, "get_async_client", forbidden_client_factory)

    backend = ClickHouseResultBackendFactory.build()

    assert calls == 0
    assert _is_new_backend(backend)
    assert tuple(inspect.signature(ClickHouseResultBackend).parameters) == (
        "host",
        "database",
        "secure",
        "result_ttl",
        "purge_ttl",
        "port",
        "username",
        "password",
        "access_token",
        "ca_cert",
        "client_cert",
        "client_cert_key",
        "server_host_name",
        "connect_timeout",
        "send_receive_timeout",
        "namespace",
        "result_table",
        "progress_table",
        "keep_results",
        "serializer",
        "serializer_id",
        "schema_mode",
    )


@pytest.mark.parametrize(
    "outcome",
    [
        pytest.param(1, id="non-boolean"),
        pytest.param(RuntimeError("private composition failure"), id="property-error"),
        pytest.param(_HostileFreshness(), id="hostile-class-hook"),
    ],
)
def test_cli_freshness_seam_rejects_malformed_runtime_outcomes(outcome: object) -> None:
    """Never expose or trust a malformed internal lifecycle observation."""
    backend = ClickHouseResultBackendFactory.build()
    backend._runtime = cast(  # noqa: SLF001 - focused package-private composition seam.
        "BackendRuntime",
        _MalformedFreshnessRuntime(outcome),
    )

    assert _is_new_backend(backend) is None


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"host": ""}, id="empty-host"),
        pytest.param({"host": " https://private.invalid"}, id="url-shaped-host-leading-space"),
        pytest.param({"host": "user@private.invalid"}, id="userinfo-host"),
        pytest.param({"host": "private.invalid:8123"}, id="host-with-port"),
        pytest.param({"host": "::1"}, id="unbracketed-ipv6"),
        pytest.param({"host": "[::1]"}, id="bracketed-ipv6"),
        pytest.param({"host": "bad_host.internal"}, id="underscore-host"),
        pytest.param({"host": f"{'a' * 64}.internal"}, id="overlong-dns-label"),
        pytest.param({"host": "999.999.999.999"}, id="out-of-range-ipv4-octets"),
        pytest.param({"host": "1.2.3"}, id="truncated-ipv4"),
        pytest.param({"host": "1.2.3.4.5"}, id="extra-ipv4-octet"),
        pytest.param({"database": "bad.name"}, id="qualified-database"),
        pytest.param({"result_table": METADATA_TABLE_NAME}, id="metadata-result-table"),
        pytest.param(
            {"progress_table": "taskiq_clickhouse_results"},
            id="duplicate-result-progress-table",
        ),
        pytest.param({"namespace": "bad namespace"}, id="invalid-namespace"),
        pytest.param({"secure": cast("Any", 1)}, id="non-boolean-secure"),
        pytest.param({"keep_results": cast("Any", 1)}, id="non-boolean-keep-results"),
        pytest.param({"password": cast("Any", object())}, id="non-string-password"),
        pytest.param({"port": True}, id="boolean-port"),
        pytest.param({"port": 65_536}, id="port-above-maximum"),
        pytest.param({"connect_timeout": 0}, id="zero-connect-timeout"),
        pytest.param({"result_ttl": timedelta(0)}, id="zero-result-ttl"),
        pytest.param(
            {"result_ttl": timedelta(days=7), "purge_ttl": timedelta(days=1)},
            id="purge-before-result",
        ),
        pytest.param({"result_ttl": cast("Any", 1)}, id="non-duration-result-ttl"),
        pytest.param({"schema_mode": cast("Any", "repair")}, id="unsupported-schema-mode"),
        pytest.param({"schema_mode": cast("Any", [])}, id="non-string-schema-mode"),
        pytest.param({"secure": True, "ca_cert": ""}, id="empty-ca-cert"),
    ],
)
def test_constructor_rejects_invalid_core_configuration(overrides: Mapping[str, object]) -> None:
    """Every core contract violation fails with a chain-free safe error."""
    with pytest.raises(ClickHouseConfigurationError) as error_info:
        ClickHouseResultBackendFactory.build(**overrides)

    assert error_info.value.__cause__ is None
    assert error_info.value.__context__ is None
    assert "private.invalid" not in str(error_info.value)


def test_constructor_failure_releases_credentials_and_endpoint_traceback_locals() -> None:
    """Public validation errors detach every raw constructor argument and inner frame."""
    private_endpoint = "private-constructor.internal"

    with pytest.raises(ClickHouseConfigurationError) as raised:
        ClickHouseResultBackendFactory.build(
            host=private_endpoint,
            secure=True,
            username="private-user",
            password=_PRIVATE_PASSWORD,
            access_token=_PRIVATE_TOKEN,
        )

    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None
    assert_production_traceback_excludes(
        raised.value,
        private_endpoint,
        _PRIVATE_PASSWORD,
        _PRIVATE_TOKEN,
    )


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"secure": False, "access_token": _ACCESS_TOKEN}, id="token-over-plaintext"),
        pytest.param({"secure": True, "access_token": ""}, id="empty-token"),
        pytest.param(
            {"secure": True, "username": "user", "access_token": _ACCESS_TOKEN},
            id="username-with-token",
        ),
        pytest.param(
            {"secure": True, "password": _PASSWORD, "access_token": _ACCESS_TOKEN},
            id="password-with-token",
        ),
        pytest.param(
            {"secure": True, "access_token": _ACCESS_TOKEN, "client_cert": "client.pem"},
            id="token-with-client-cert",
        ),
        pytest.param({"secure": False, "ca_cert": "ca.pem"}, id="ca-cert-over-plaintext"),
        pytest.param({"secure": True, "client_cert": "client.pem"}, id="client-cert-without-key"),
        pytest.param(
            {"secure": True, "username": "user", "password": _PASSWORD, "client_cert": "client.pem"},
            id="client-cert-with-basic-auth",
        ),
        pytest.param({"secure": True, "client_cert_key": "key.pem"}, id="client-key-without-cert"),
    ],
)
def test_constructor_rejects_invalid_auth_and_tls(overrides: Mapping[str, object]) -> None:
    """TLS and authentication modes remain mutually exclusive and explicit."""
    with pytest.raises(ClickHouseConfigurationError):
        ClickHouseResultBackendFactory.build(**overrides)


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        pytest.param(
            {"secure": False, "username": "", "password": _PASSWORD},
            AuthenticationConfig(
                username=None,
                password=_PASSWORD,
                access_token=None,
                ca_cert=None,
                client_cert=None,
                client_cert_key=None,
                server_host_name=None,
            ),
            id="default-user-password",
        ),
        pytest.param(
            {
                "secure": True,
                "access_token": _ACCESS_TOKEN,
                "ca_cert": "ca.pem",
                "server_host_name": "clickhouse.internal",
            },
            AuthenticationConfig(
                username=None,
                password="",
                access_token=_ACCESS_TOKEN,
                ca_cert="ca.pem",
                client_cert=None,
                client_cert_key=None,
                server_host_name="clickhouse.internal",
            ),
            id="access-token",
        ),
        pytest.param(
            {
                "secure": True,
                "username": "user",
                "client_cert": "client.pem",
                "client_cert_key": "key.pem",
            },
            AuthenticationConfig(
                username="user",
                password="",
                access_token=None,
                ca_cert=None,
                client_cert="client.pem",
                client_cert_key="key.pem",
                server_host_name=None,
            ),
            id="mutual-tls",
        ),
    ],
)
def test_constructor_accepts_each_frozen_auth_mode(
    monkeypatch: pytest.MonkeyPatch,
    overrides: Mapping[str, object],
    expected: AuthenticationConfig,
) -> None:
    """Basic/default-user, token and mutual TLS modes retain normalized values."""
    capture = _install_composition_capture(monkeypatch)
    ClickHouseResultBackendFactory.build(**overrides)

    assert capture.configs[0].authentication == expected


def test_serializer_identity_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    """Exact built-ins receive reserved ids and custom serializers require ids."""
    capture = _install_composition_capture(monkeypatch)
    exact_json = JSONSerializer(default=None, ensure_ascii=True)
    exact_pickle = PickleSerializer()
    custom = _CustomSerializer()

    ClickHouseResultBackendFactory.build(
        serializer=exact_json,
        serializer_id="taskiq-json-v1",
    )
    ClickHouseResultBackendFactory.build(serializer=exact_pickle)
    ClickHouseResultBackendFactory.build(
        serializer=custom,
        serializer_id="application-v1",
    )

    captured_json, captured_pickle, captured_custom = capture.serializers
    assert type(captured_json) is JSONSerializer
    assert captured_json is not exact_json
    assert captured_json.default is None
    assert captured_json.ensure_ascii is True
    assert type(captured_pickle) is PickleSerializer
    assert captured_pickle is not exact_pickle
    assert captured_custom is custom
    assert [config.storage.serializer_id for config in capture.configs] == [
        "taskiq-json-v1",
        "taskiq-pickle-v1",
        "application-v1",
    ]


def test_builtin_serializer_instances_are_isolated_from_caller_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep reserved built-in ids bound to package-owned canonical behavior."""
    capture = _install_composition_capture(monkeypatch)
    caller_json = JSONSerializer(default=None, ensure_ascii=True)
    caller_pickle = PickleSerializer()

    ClickHouseResultBackendFactory.build(serializer=caller_json)
    ClickHouseResultBackendFactory.build(serializer=caller_pickle)
    caller_json.ensure_ascii = False
    caller_json.default = lambda _value: None
    caller_pickle.__dict__["dumpb"] = lambda _value: b"caller-mutated"

    captured_json, captured_pickle = capture.serializers
    assert captured_json.dumpb({"snowman": "\u2603"}) == b'{"snowman": "\\u2603"}'
    with pytest.raises(TypeError):
        captured_json.dumpb({"unsupported": {1}})
    assert captured_pickle.dumpb({"value": 1}) != b"caller-mutated"


def test_custom_serializer_validation_does_not_read_instance_class_hooks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Accept a real subclass without invoking its untrusted ``__class__`` hook."""
    capture = _install_composition_capture(monkeypatch)
    serializer = _HostileSerializerClassLookup()

    ClickHouseResultBackendFactory.build(
        serializer=serializer,
        serializer_id="application-v1",
    )

    assert capture.serializers == [serializer]


def test_production_composition_shares_one_serializer_admission() -> None:
    """Serialize result and progress calls made through one backend instance."""
    serializer = _CustomSerializer()

    components = backend_composition.compose_backend(
        BackendConfigFactory.build(),
        serializer,
    )

    assert components.result_codec.serializer is serializer
    assert components.progress_codec.serializer is serializer
    assert components.result_codec.serializer_admission is components.progress_codec.serializer_admission


@pytest.mark.parametrize(
    ("serializer", "serializer_id"),
    [
        pytest.param(JSONSerializer(), "different-v1", id="json-with-custom-id"),
        pytest.param(PickleSerializer(), "different-v1", id="pickle-with-custom-id"),
        pytest.param(_CustomSerializer(), None, id="custom-without-id"),
        pytest.param(_CustomSerializer(), "taskiq-json-v1", id="custom-with-reserved-id"),
        pytest.param(_CustomSerializer(), "bad:id", id="custom-with-malformed-id"),
        pytest.param(JSONSerializer(ensure_ascii=False), None, id="noncanonical-json"),
        pytest.param(_JsonSubclass(), None, id="json-subclass-without-id"),
        pytest.param(cast("Any", object()), "application-v1", id="non-serializer-object"),
    ],
)
def test_serializer_identity_rejects_ambiguous_configuration(
    serializer: TaskiqSerializer,
    serializer_id: str | None,
) -> None:
    """Serializer configurations cannot silently reuse incompatible identities."""
    with pytest.raises(ClickHouseConfigurationError):
        ClickHouseResultBackendFactory.build(
            serializer=serializer,
            serializer_id=serializer_id,
        )


@pytest.mark.parametrize(
    "serializer_id",
    [
        pytest.param(_HostileSerializerId(), id="custom-equality-hook"),
        pytest.param(_SerializerIdSubclass("taskiq-json-v1"), id="str-subclass"),
    ],
)
def test_builtin_serializer_id_rejects_hostile_values_before_equality(
    serializer_id: object,
) -> None:
    """Fail closed without executing comparison hooks or leaking their text."""
    secret = "password=serializer-id-secret"  # noqa: S105  # pragma: allowlist secret

    with pytest.raises(ClickHouseConfigurationError, match="built_in_serializer_id_mismatch") as raised:
        ClickHouseResultBackendFactory.build(
            serializer_id=cast("Any", serializer_id),
        )

    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None
    assert secret not in repr(raised.value) + str(raised.value)


def test_constructor_distinguishes_ttl_range_from_ttl_order() -> None:
    """Report an ordered but unrepresentable duration with the correct code."""
    with pytest.raises(ClickHouseConfigurationError, match="ttl_range") as raised:
        ClickHouseResultBackendFactory.build(purge_ttl=timedelta.max)

    assert raised.value.reason == "ttl_range"
    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None


@pytest.mark.asyncio
async def test_client_factory_owns_all_correctness_and_security_options(monkeypatch: pytest.MonkeyPatch) -> None:
    """The deferred factory passes only the frozen package-owned driver options."""
    captured: dict[str, object] = {}
    fake_client = _FakeClient()

    async def create_client(**kwargs: object) -> AsyncClient:
        """Capture the exact deferred driver call."""
        captured.update(kwargs)
        return cast("AsyncClient", fake_client)

    monkeypatch.setattr(clickhouse_connect, "get_async_client", create_client)

    client = await lifecycle_module.create_client(BackendConfigFactory.build())

    assert cast("object", client) is fake_client
    assert captured == {
        "host": "localhost",
        "port": None,
        "username": None,
        "password": "",
        "access_token": None,
        "database": "tasks",
        "interface": "http",
        "secure": False,
        "verify": True,
        "ca_cert": None,
        "client_cert": None,
        "client_cert_key": None,
        "server_host_name": None,
        "connect_timeout": 10,
        "send_receive_timeout": 300,
        "tz_mode": "aware",
        "autogenerate_session_id": False,
        "query_retries": 2,
        "client_name": f"taskiq-clickhouse/{distribution_version('taskiq-clickhouse')}",
    }


def test_taskiq_model_contract_is_checked_at_construction(monkeypatch: pytest.MonkeyPatch) -> None:
    """A resolved incompatible Taskiq model fails before any client creation."""
    with monkeypatch.context() as context:
        context.setattr(TaskiqResult, "model_fields", {})
        with pytest.raises(ClickHouseConfigurationError):
            ClickHouseResultBackendFactory.build()
    with monkeypatch.context() as context:
        context.setattr(TaskProgress, "model_fields", {})
        with pytest.raises(ClickHouseConfigurationError):
            ClickHouseResultBackendFactory.build()


@pytest.mark.parametrize(
    "replacement",
    [
        pytest.param(
            SimpleNamespace(annotation=str, default=None, default_factory=None, is_required=lambda: False),
            id="non-optional-annotation",
        ),
        pytest.param(
            SimpleNamespace(annotation=str | None, default="", default_factory=None, is_required=lambda: False),
            id="non-null-default",
        ),
        pytest.param(
            SimpleNamespace(annotation=str | None, default=None, default_factory=None, is_required=lambda: True),
            id="required-field",
        ),
    ],
)
def test_taskiq_log_field_contract_rejects_in_place_changes(
    monkeypatch: pytest.MonkeyPatch,
    replacement: object,
) -> None:
    """Detect a changed deprecated log field even when its name remains present."""
    changed_fields = {**TaskiqResult.model_fields, "log": replacement}
    monkeypatch.setattr(TaskiqResult, "model_fields", changed_fields)

    with pytest.raises(ClickHouseConfigurationError, match="unsupported_taskiq_result_model"):
        ClickHouseResultBackendFactory.build()


def test_taskiq_progress_state_annotation_change_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pin the persisted progress-state union rather than only its field name."""
    replacement = SimpleNamespace(
        annotation=int,
        default=None,
        default_factory=None,
        is_required=lambda: True,
    )
    changed_fields = {**TaskProgress.model_fields, "state": replacement}
    monkeypatch.setattr(TaskProgress, "model_fields", changed_fields)

    with pytest.raises(ClickHouseConfigurationError, match="unsupported_taskiq_progress_model"):
        ClickHouseResultBackendFactory.build()


@pytest.mark.parametrize(
    "model_fields",
    [
        pytest.param(None, id="none"),
        pytest.param(object(), id="non-mapping-object"),
        pytest.param(["is_err"], id="sequence"),
        pytest.param({object(): object()}, id="non-string-field-name"),
    ],
)
def test_taskiq_model_contract_malformed_shape_is_safely_rejected(
    monkeypatch: pytest.MonkeyPatch,
    model_fields: object,
) -> None:
    """Never leak dependency AttributeError or TypeError from construction."""
    monkeypatch.setattr(TaskiqResult, "model_fields", model_fields)

    with pytest.raises(ClickHouseConfigurationError, match="unsupported_taskiq_result_model"):
        ClickHouseResultBackendFactory.build()


def test_taskiq_model_field_metadata_failure_is_safely_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Translate malformed dependency FieldInfo objects without raw context."""
    malformed_fields = {**TaskiqResult.model_fields, "log": object()}
    monkeypatch.setattr(TaskiqResult, "model_fields", malformed_fields)

    with pytest.raises(ClickHouseConfigurationError, match="unsupported_taskiq_result_model") as raised:
        ClickHouseResultBackendFactory.build()

    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None


def test_taskiq_model_field_mapping_failure_is_safely_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Snapshot dependency mappings before their hooks can escape the boundary."""
    monkeypatch.setattr(TaskiqResult, "model_fields", _ExplodingModelFields())

    with pytest.raises(ClickHouseConfigurationError, match="unsupported_taskiq_result_model") as raised:
        ClickHouseResultBackendFactory.build()

    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None


@pytest.mark.parametrize(
    "annotation",
    [
        pytest.param(_ExplodingAnnotation(), id="throwing-equality"),
        pytest.param(_NonBooleanAnnotation(), id="non-boolean-equality"),
    ],
)
def test_taskiq_model_annotation_equality_is_safely_rejected(
    monkeypatch: pytest.MonkeyPatch,
    annotation: object,
) -> None:
    """Accept only an exact true result from dependency annotation equality."""
    replacement = SimpleNamespace(
        annotation=annotation,
        default=None,
        default_factory=None,
        is_required=lambda: False,
    )
    monkeypatch.setattr(TaskiqResult, "model_fields", {**TaskiqResult.model_fields, "log": replacement})

    with pytest.raises(ClickHouseConfigurationError, match="unsupported_taskiq_result_model") as raised:
        ClickHouseResultBackendFactory.build()

    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None


def test_internal_configuration_reprs_redact_connection_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep private endpoints and certificate paths out of nested reprs."""
    capture = _install_composition_capture(monkeypatch)
    ClickHouseResultBackendFactory.build(
        host="private.internal",
        secure=True,
        ca_cert="/private/ca.pem",
        server_host_name="clickhouse.private.internal",
    )

    rendered = repr(capture.configs[0])
    assert "private.internal" not in rendered
    assert "/private/ca.pem" not in rendered


def test_explicit_valid_port_is_retained(monkeypatch: pytest.MonkeyPatch) -> None:
    """A non-default port within the frozen range remains unchanged."""
    capture = _install_composition_capture(monkeypatch)
    ClickHouseResultBackendFactory.build(
        host="127.0.0.1",
        port=1,
    )

    assert capture.configs[0].endpoint.port == 1


def test_valid_dns_fqdn_is_retained(monkeypatch: pytest.MonkeyPatch) -> None:
    """A conventional hyphenated absolute DNS name is accepted unchanged."""
    capture = _install_composition_capture(monkeypatch)
    ClickHouseResultBackendFactory.build(
        host="click-house.internal.",
    )

    assert capture.configs[0].endpoint.host == "click-house.internal."


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "operation",
    [
        pytest.param(
            lambda backend: backend.set_result(
                "task",
                cast("TaskiqResult[object]", object()),
            ),
            id="set-result",
        ),
        pytest.param(
            lambda backend: backend.is_result_ready("task"),
            id="is-result-ready",
        ),
        pytest.param(
            lambda backend: backend.get_result("task"),
            id="get-result",
        ),
        pytest.param(
            lambda backend: backend.set_progress("task", cast("Any", object())),
            id="set-progress",
        ),
        pytest.param(
            lambda backend: backend.get_progress("task"),
            id="get-progress",
        ),
    ],
)
async def test_new_data_methods_fail_before_io(
    operation: Callable[[ClickHouseResultBackend[object]], Awaitable[object]],
) -> None:
    """Reject each data operation independently while the backend is NEW."""
    backend = ClickHouseResultBackendFactory.build()

    with pytest.raises(ClickHouseLifecycleError):
        await operation(backend)


@pytest.mark.asyncio
async def test_real_composition_builds_ready_repository_at_startup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exercise the concrete adapter/repository wiring through public methods."""
    client = _CompositionClient()

    async def create_client(_config: BackendConfig) -> AsyncClient:
        return cast("AsyncClient", client)

    async def run_schema_barrier(
        _context: object,
        observed_client: AsyncClient,
        **_policy: object,
    ) -> None:
        assert observed_client is cast("object", client)

    monkeypatch.setattr(backend_composition, "create_client", create_client)
    monkeypatch.setattr(
        backend_composition,
        "run_schema_barrier",
        run_schema_barrier,
    )
    backend = ClickHouseResultBackendFactory.build()

    await backend.startup()
    try:
        assert not await backend.is_result_ready("missing")
    finally:
        await backend.shutdown()

    assert client.query_calls == 1
    assert client.close_calls == 1


@pytest.mark.asyncio
async def test_public_get_result_returns_the_delegated_generic_model(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exercise result selection and decoding through one composition seam."""
    expected = TaskiqResult[Any](
        is_err=False,
        log=None,
        return_value={"value": 1},
        execution_time=0.25,
        labels={},
        error=None,
    )
    store = _ResultStoreProbe()
    codec = _ResultCodecProbe(expected)

    def compose(
        config: BackendConfig,
        serializer: TaskiqSerializer,
    ) -> BackendComponents:
        del config
        return BackendComponents(
            runtime=cast("BackendRuntime", _ReadyRuntime(store)),
            result_codec=cast("ResultCodec", codec),
            progress_codec=ProgressCodec(serializer),
            keep_results=True,
        )

    monkeypatch.setattr(backend_module, "compose_backend", compose)
    backend = ClickHouseResultBackendFactory.build()

    observed = await backend.get_result("task", with_logs=True)

    assert observed is expected
    assert store.calls == [("read-with-log", "task")]
    assert codec.calls == [(b"stored-result", b"stored-log")]
