"""Taskiq serializer identity and model compatibility boundary."""

from dataclasses import dataclass
import re
import typing

from taskiq.abc.serializer import TaskiqSerializer
from taskiq.depends.progress_tracker import TaskProgress, TaskState
from taskiq.result import TaskiqResult
from taskiq.serializers.json_serializer import JSONSerializer
from taskiq.serializers.pickle import PickleSerializer

from taskiq_clickhouse.exceptions import ClickHouseConfigurationError


@dataclass(frozen=True, slots=True)
class ModelFieldContract:
    """Stable Pydantic field facts consumed by this Taskiq adapter."""

    name: str
    annotation: object | None
    required: bool
    default_factory: object | None = None
    default: object | None = None


@dataclass(frozen=True, slots=True)
class _ModelFieldSnapshot:
    annotation: object
    required: bool
    default_factory: object | None
    default: object


_INVALID_SERIALIZER = "invalid_serializer"
_BUILTIN_ID_MISMATCH = "built_in_serializer_id_mismatch"
_INVALID_CUSTOM_ID = "invalid_custom_serializer_id"
_UNSUPPORTED_RESULT_MODEL = "unsupported_taskiq_result_model"
_UNSUPPORTED_PROGRESS_MODEL = "unsupported_taskiq_progress_model"
RESULT_LOG_FIELD_NAME: typing.Final = "log"
RESULT_MODEL_CONTRACTS: typing.Final = (
    ModelFieldContract(name="is_err", annotation=bool, required=True),
    ModelFieldContract(name=RESULT_LOG_FIELD_NAME, annotation=str | None, required=False),
    ModelFieldContract(name="return_value", annotation=None, required=True),
    ModelFieldContract(name="execution_time", annotation=float, required=True),
    ModelFieldContract(
        name="labels",
        annotation=dict[str, typing.Any],
        required=False,
        default_factory=dict,
    ),
    ModelFieldContract(name="error", annotation=BaseException | None, required=False),
)
PROGRESS_MODEL_CONTRACTS: typing.Final = (
    ModelFieldContract(name="state", annotation=TaskState | str, required=True),
    ModelFieldContract(name="meta", annotation=None, required=True),
)
RESULT_MODEL_FIELD_NAMES: typing.Final = tuple(contract.name for contract in RESULT_MODEL_CONTRACTS)
RESULT_PAYLOAD_FIELD_NAMES: typing.Final = tuple(
    name for name in RESULT_MODEL_FIELD_NAMES if name != RESULT_LOG_FIELD_NAME
)
PROGRESS_MODEL_FIELD_NAMES: typing.Final = tuple(contract.name for contract in PROGRESS_MODEL_CONTRACTS)


class SerializerIdentity:
    """Assign durable ids to built-in and explicitly versioned serializers."""

    _id_pattern = re.compile(r"[A-Za-z][A-Za-z0-9._-]{0,63}\Z")
    _json_id = "taskiq-json-v1"
    _pickle_id = "taskiq-pickle-v1"
    _reserved_ids = frozenset((_json_id, _pickle_id))

    def resolve(
        self,
        serializer: object,
        serializer_id: object,
    ) -> tuple[TaskiqSerializer, str]:
        """Return one serializer strategy and its stable storage identity."""
        if serializer is None:
            default_serializer = JSONSerializer(default=None, ensure_ascii=True)
            return default_serializer, self._builtin_id(serializer_id, self._json_id)
        if not issubclass(type(serializer), TaskiqSerializer):
            raise _configuration_error(_INVALID_SERIALIZER)
        typed_serializer = typing.cast("TaskiqSerializer", serializer)
        if self._is_default_json(typed_serializer):
            canonical_json = JSONSerializer(default=None, ensure_ascii=True)
            return canonical_json, self._builtin_id(serializer_id, self._json_id)
        if type(typed_serializer) is PickleSerializer:  # noqa: WPS516 - subclasses require explicit ids.
            return PickleSerializer(), self._builtin_id(serializer_id, self._pickle_id)
        return typed_serializer, self._custom_id(serializer_id)

    def _is_default_json(self, serializer: TaskiqSerializer) -> bool:
        if type(serializer) is not JSONSerializer:  # noqa: WPS516 - subclasses require explicit ids.
            return False
        return serializer.default is None and serializer.ensure_ascii is True

    def _builtin_id(self, candidate: object, expected: str) -> str:
        if candidate is None:
            return expected
        exact_expected_id = (
            type(candidate) is str  # noqa: WPS516 - persistent ids reject string subclasses.
            and candidate == expected
        )
        if not exact_expected_id:
            raise _configuration_error(_BUILTIN_ID_MISMATCH)
        return expected

    def _custom_id(self, candidate: object) -> str:
        if type(candidate) is not str:  # noqa: WPS516 - persistent ids require exact strings.
            raise _configuration_error(_INVALID_CUSTOM_ID)
        invalid_pattern = self._id_pattern.fullmatch(candidate) is None
        if invalid_pattern or candidate in self._reserved_ids:
            raise _configuration_error(_INVALID_CUSTOM_ID)
        return candidate


class TaskiqModelCompatibility:
    """Fail closed when Taskiq models no longer match consumed fields."""

    def verify(self) -> None:
        """Validate the exact Taskiq result and progress model field sets."""
        self._verify_model(
            TaskiqResult,
            RESULT_MODEL_CONTRACTS,
            reason=_UNSUPPORTED_RESULT_MODEL,
        )
        self._verify_model(
            TaskProgress,
            PROGRESS_MODEL_CONTRACTS,
            reason=_UNSUPPORTED_PROGRESS_MODEL,
        )

    def _verify_model(
        self,
        model: object,
        contracts: tuple[ModelFieldContract, ...],
        *,
        reason: str,
    ) -> None:
        fields = self._model_fields(model, reason=reason)
        expected_names = frozenset(contract.name for contract in contracts)
        if frozenset(fields) != expected_names:
            raise _configuration_error(reason)
        if not all(self._field_matches(fields, contract) for contract in contracts):
            raise _configuration_error(reason)

    def _model_fields(self, model: object, *, reason: str) -> dict[str, object]:
        fields = self._read_model_fields(model)
        if fields is None:
            raise _configuration_error(reason)
        return fields

    def _read_model_fields(self, model: object) -> dict[str, object] | None:
        """Copy unstable dependency metadata without retaining its exception."""
        try:
            fields = getattr(model, "model_fields")  # noqa: B009 - dependency attribute may be absent.
            if not isinstance(fields, typing.Mapping):
                return None
            copied = dict(fields)
            exact_names = all(
                type(field_name) is str  # noqa: WPS516 - dependency keys are an exact contract.
                for field_name in copied
            )
        except Exception:  # noqa: BLE001 - incompatible dependency shape is classified below.
            return None
        if not exact_names:
            return None
        return typing.cast("dict[str, object]", copied)

    def _field_matches(
        self,
        fields: typing.Mapping[str, object],
        contract: ModelFieldContract,
    ) -> bool:
        snapshot = self._field_snapshot(fields, contract.name)
        if snapshot is None:
            return False
        annotation_matches = _annotation_matches(snapshot.annotation, contract.annotation)
        default_matches = (
            contract.required or contract.default_factory is not None or snapshot.default is contract.default
        )
        return all(
            (
                annotation_matches,
                snapshot.required is contract.required,
                snapshot.default_factory is contract.default_factory,
                default_matches,
            ),
        )

    def _field_snapshot(
        self,
        fields: typing.Mapping[str, object],
        field_name: str,
    ) -> _ModelFieldSnapshot | None:
        """Read unstable Pydantic metadata without leaking dependency errors."""
        try:
            model_field = typing.cast("typing.Any", fields[field_name])
            return _ModelFieldSnapshot(
                annotation=model_field.annotation,
                required=model_field.is_required(),
                default_factory=model_field.default_factory,
                default=model_field.default,
            )
        except Exception:  # noqa: BLE001 - incompatible dependency metadata is a false match.
            return None


SERIALIZER_IDENTITY = SerializerIdentity()
TASKIQ_MODELS = TaskiqModelCompatibility()


def _configuration_error(reason: str) -> ClickHouseConfigurationError:
    return ClickHouseConfigurationError("configuration", reason)


def _annotation_matches(candidate: object, expected: object | None) -> bool:
    """Compare dependency metadata without trusting its equality result."""
    if expected is None:
        return True
    try:
        comparison = candidate == expected
    except Exception:  # noqa: BLE001 - malformed dependency metadata is a false match.
        return False
    return comparison is True
