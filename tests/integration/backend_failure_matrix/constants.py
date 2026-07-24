"""Stable values for the authentication and permission matrix."""

from typing import Final


AUTH_PASSWORD: Final = "auth-correct-password"  # noqa: S105 - deterministic integration credential.
AUTH_WRONG_PASSWORD: Final = "auth-wrong-password"  # noqa: S105 - deterministic invalid credential.
SCHEMA_PASSWORD: Final = "schema-read-password"  # noqa: S105 - deterministic integration credential.
READ_ONLY_PASSWORD: Final = "read-only-password"  # noqa: S105 - deterministic integration credential.
REVOKED_PASSWORD: Final = "revoked-read-password"  # noqa: S105 - deterministic integration credential.

MISSING_TASK_ID: Final = "permission-missing-result"
DENIED_TASK_ID: Final = "permission-denied-result"
SECRET_PAYLOAD: Final = "PRIVATE_RESULT_PAYLOAD"  # noqa: S105 - payload redaction sentinel.
SECRET_TOKEN: Final = "PRIVATE_RESULT_TOKEN"  # noqa: S105 - redaction sentinel, not a credential.
SECRET_LOG: Final = "PRIVATE_TASK_LOG"  # noqa: S105 - task-log redaction sentinel.
