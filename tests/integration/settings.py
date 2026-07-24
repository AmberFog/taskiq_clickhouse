"""Validate the explicit environment for ClickHouse integration tests."""

from dataclasses import dataclass
import ipaddress
import os
from pathlib import Path
from typing import Final, Literal, TypeAlias, cast
from uuid import uuid4


ClickHouseProfile: TypeAlias = Literal["minimum", "current"]

HOST_ENV: Final = "TASKIQ_CLICKHOUSE_HOST"
PORT_ENV: Final = "TASKIQ_CLICKHOUSE_PORT"
USER_ENV: Final = "TASKIQ_CLICKHOUSE_USER"
PASSWORD_ENV: Final = "TASKIQ_CLICKHOUSE_PASSWORD"  # noqa: S105  # Environment-variable name, not a credential.
PROFILE_ENV: Final = "TASKIQ_CLICKHOUSE_PROFILE"
VERSION_ENV: Final = "TASKIQ_CLICKHOUSE_EXPECTED_VERSION"
CLIENT_VERSION_ENV: Final = "TASKIQ_CLICKHOUSE_EXPECTED_CLIENT_VERSION"
ASYNC_INSERT_ENV: Final = "TASKIQ_CLICKHOUSE_EXPECTED_ASYNC_INSERT"
EVIDENCE_DIR_ENV: Final = "TASKIQ_CLICKHOUSE_EVIDENCE_DIR"
XDIST_RUN_ENV: Final = "PYTEST_XDIST_TESTRUNUID"
XDIST_WORKER_ENV: Final = "PYTEST_XDIST_WORKER"

ENVIRONMENT_NAMES: Final = (
    HOST_ENV,
    PORT_ENV,
    USER_ENV,
    PASSWORD_ENV,
    PROFILE_ENV,
    VERSION_ENV,
    CLIENT_VERSION_ENV,
    ASYNC_INSERT_ENV,
    EVIDENCE_DIR_ENV,
)
ALLOWED_PROFILES: Final = frozenset({"minimum", "current"})
MIN_PORT: Final = 1
MAX_PORT: Final = 65_535
DATABASE_PREFIX: Final = "taskiq_ch"


@dataclass(frozen=True, slots=True)
class ClickHouseTestSettings:
    """Validated connection and expectations supplied by the test owner."""

    host: str
    port: int
    username: str
    password: str
    profile: ClickHouseProfile
    expected_version: str
    expected_client_version: str
    expected_async_insert: int
    evidence_dir: Path


def load_clickhouse_settings() -> ClickHouseTestSettings:
    """Load required values without providing hidden local defaults."""
    values = _required_environment()
    host = _parse_host(values[HOST_ENV])
    port = _parse_port(values[PORT_ENV])
    profile = _parse_profile(values[PROFILE_ENV])
    expected_async_insert = _parse_async_insert(values[ASYNC_INSERT_ENV])
    evidence_dir = Path(values[EVIDENCE_DIR_ENV])
    if not evidence_dir.is_absolute():
        msg = f"{EVIDENCE_DIR_ENV} must be an absolute path"
        raise ValueError(msg)

    return ClickHouseTestSettings(
        host=host,
        port=port,
        username=values[USER_ENV],
        password=values[PASSWORD_ENV],
        profile=profile,
        expected_version=values[VERSION_ENV],
        expected_client_version=values[CLIENT_VERSION_ENV],
        expected_async_insert=expected_async_insert,
        evidence_dir=evidence_dir,
    )


def make_worker_database_name(settings: ClickHouseTestSettings) -> str:
    """Build a collision-resistant identifier shared only by one pytest worker."""
    run_id = os.environ.get(XDIST_RUN_ENV, uuid4().hex)
    worker_id = os.environ.get(XDIST_WORKER_ENV, "main")
    safe_run_id = _identifier_fragment(run_id)[:16]
    safe_worker_id = _identifier_fragment(worker_id)[:16]
    return f"{DATABASE_PREFIX}_{settings.profile}_{safe_run_id}_{safe_worker_id}"


def _required_environment() -> dict[str, str]:
    values = {name: os.environ.get(name, "") for name in ENVIRONMENT_NAMES}
    missing = sorted(name for name, value in values.items() if not value)
    if missing:
        msg = f"missing required integration environment: {', '.join(missing)}"
        raise ValueError(msg)
    return values


def _parse_port(raw_port: str) -> int:
    try:
        port = int(raw_port)
    except ValueError as error:
        msg = f"{PORT_ENV} must be an integer"
        raise ValueError(msg) from error
    if not MIN_PORT <= port <= MAX_PORT:
        msg = f"{PORT_ENV} must be between {MIN_PORT} and {MAX_PORT}"
        raise ValueError(msg)
    return port


def _parse_host(raw_host: str) -> str:
    host_text = raw_host.removeprefix("[").removesuffix("]")
    try:
        host = ipaddress.ip_address(host_text)
    except ValueError as error:
        msg = f"{HOST_ENV} must be a loopback IP literal"
        raise ValueError(msg) from error
    if not host.is_loopback:
        msg = f"{HOST_ENV} must be loopback"
        raise ValueError(msg)
    return str(host)


def _parse_profile(raw_profile: str) -> ClickHouseProfile:
    if raw_profile not in ALLOWED_PROFILES:
        msg = f"{PROFILE_ENV} must be minimum or current"
        raise ValueError(msg)
    return cast("ClickHouseProfile", raw_profile)


def _parse_async_insert(raw_value: str) -> int:
    if raw_value not in {"0", "1"}:
        msg = f"{ASYNC_INSERT_ENV} must be 0 or 1"
        raise ValueError(msg)
    return int(raw_value)


def _identifier_fragment(value: str) -> str:
    fragment = "".join(character if character.isalnum() else "_" for character in value)
    return fragment or "unknown"
