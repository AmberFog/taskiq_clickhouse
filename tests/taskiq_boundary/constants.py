"""Stable values for Taskiq public-boundary scenarios."""

from collections.abc import Mapping
from datetime import timedelta
import logging
from types import MappingProxyType
from typing import Final


RESULT_TTL: Final = timedelta(hours=1)
PURGE_TTL: Final = timedelta(days=1)

UNIT_HOST: Final = "private.invalid"
UNIT_USERNAME: Final = "boundary-user"
UNIT_PASSWORD: Final = "boundary-password"  # noqa: S105 - inert unit-test credential.
SECRET_RETURN_VALUE: Final = "boundary-private-return"  # noqa: S105 - redaction sentinel, not a credential.
SECRET_TASK_LOG: Final = "boundary-private-log"  # noqa: S105 - redaction sentinel, not a credential.
DIRECT_EXECUTION_TIME: Final = 0.25

RECEIVER_FAILURE_TASK: Final = "tests.taskiq_boundary:receiver_failure"
RECEIVER_FAILURE_TASK_ID: Final = "receiver-failure"
PERSISTENCE_RECEIVER_TASK: Final = "tests:result-persistence-receiver"
PERSISTENCE_RECEIVER_TASK_ID: Final = "persistence-gated-receiver-task-id"
DIRECT_FAILURE_TASK_ID: Final = "direct-failure"
WRAPPER_TASK_ID: Final = "wrapper-failure"

SUCCESS_TASK_NAME: Final = "tests.taskiq_boundary:typed_success"
ERROR_TASK_NAME: Final = "tests.taskiq_boundary:typed_error"
MISSING_TASK_ID: Final = "taskiq-boundary-missing"
SUCCESS_VALUE: Final = 42
SUCCESS_LABEL_SOURCE: Final = "taskiq-boundary"
SUCCESS_LABEL_ATTEMPT: Final = 7
EXPECTED_TASK_LABELS: Final[Mapping[str, object]] = MappingProxyType(
    {
        "source": SUCCESS_LABEL_SOURCE,
        "attempt": SUCCESS_LABEL_ATTEMPT,
    },
)
TASK_ERROR_MESSAGE: Final = "receiver-task-error"
TASK_ERROR_CAUSE: Final = "receiver-task-cause"
BOUNDARY_TIMEOUT_SECONDS: Final = 10.0

TASKIQ_RECEIVER_LOGGER: Final = "taskiq.receiver.receiver"
RECEIVER_ERROR_LEVEL: Final = logging.ERROR
SAFE_NOT_READY_MESSAGE: Final = "ClickHouse operation failed [backend:not_ready]"
REDACTED_RECEIVER_VALUES: Final = (
    UNIT_HOST,
    UNIT_USERNAME,
    UNIT_PASSWORD,
    SECRET_RETURN_VALUE,
    SECRET_TASK_LOG,
)
