"""Build one validated backend configuration without side effects."""

from taskiq.abc.serializer import TaskiqSerializer

from taskiq_clickhouse._config_input import RawBackendConfig
from taskiq_clickhouse._config_models import BackendConfig
from taskiq_clickhouse._config_validation import CONFIGURATION_VALIDATOR
from taskiq_clickhouse._taskiq_compat import SERIALIZER_IDENTITY, TASKIQ_MODELS


def validate_backend_configuration(
    raw: RawBackendConfig,
    serializer: object,
) -> tuple[BackendConfig, TaskiqSerializer]:
    """Resolve Taskiq compatibility and validate every constructor policy."""
    resolved_serializer, serializer_id = SERIALIZER_IDENTITY.resolve(
        serializer,
        raw.storage.serializer_id,
    )
    TASKIQ_MODELS.verify()
    config = CONFIGURATION_VALIDATOR.validate(
        raw,
        serializer_id=serializer_id,
    )
    return config, resolved_serializer
