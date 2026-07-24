"""Verify public client-startup failures through real TCP behavior."""

from datetime import timedelta

import pytest

from taskiq_clickhouse.backend import ClickHouseResultBackend
from taskiq_clickhouse.exceptions import ClickHouseBackendIOError
from tests.result_contract.assertions import assert_safe_public_error
from tests.result_contract.network_actions import NetworkMode, reserved_endpoint


_PASSWORD = "PRIVATE_PASSWORD"  # noqa: S105 - redaction sentinel.  # pragma: allowlist secret
_USERNAME = "PRIVATE_USERNAME"
_ENDPOINT = "127.0.0.1"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mode",
    [
        pytest.param("refused", id="service-unavailable"),
        pytest.param("timeout", id="receive-timeout"),
    ],
)
async def test_network_startup_failure_is_safe_and_never_ready(mode: NetworkMode) -> None:
    """Classify refused and non-responsive TCP endpoints without raw details."""
    with reserved_endpoint(mode) as port:
        backend = ClickHouseResultBackend[object](
            host=_ENDPOINT,
            port=port,
            username=_USERNAME,
            password=_PASSWORD,
            database="tasks",
            secure=False,
            connect_timeout=1,
            send_receive_timeout=1,
            result_ttl=timedelta(seconds=1),
            purge_ttl=timedelta(seconds=2),
        )

        with pytest.raises(ClickHouseBackendIOError) as raised:
            await backend.startup()

    for forbidden in (_PASSWORD, _USERNAME, _ENDPOINT, str(port)):
        assert_safe_public_error(
            raised.value,
            operation="backend",
            reason="client_create_failed",
            forbidden=forbidden,
        )
