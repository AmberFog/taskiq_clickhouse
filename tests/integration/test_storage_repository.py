"""Exercise the production storage repository against real ClickHouse."""

import asyncio
from datetime import timedelta
from typing import Final

from clickhouse_connect.driver.asyncclient import AsyncClient
import pytest

from taskiq_clickhouse._storage.result_records import TOMBSTONE_STATE
from tests.integration.fixtures import ClickHouseClientFactory
from tests.integration.storage_repository_contract import (
    record_builders,
    repository_actions,
    table_io,
)
from tests.integration.storage_repository_contract.assertions import (
    assert_opaque_round_trip,
)
from tests.integration.storage_repository_contract.cases import (
    OPAQUE_CASES,
    OpaqueCase,
)
from tests.integration.storage_repository_contract.gateways import (
    EqualObservationGateway,
    ResponseLossGateway,
)
from tests.integration.storage_repository_contract.response_loss import (
    RESPONSE_LOSS_PARAMETERS,
    ResponseLossCase,
)


pytestmark = [pytest.mark.clickhouse, pytest.mark.asyncio]

_SCENARIO_TIMEOUT_SECONDS: Final = 18
_CONCURRENT_WRITERS: Final = 8


@pytest.mark.parametrize("opaque_case", OPAQUE_CASES, ids=lambda case: case.name)
async def test_opaque_bytes_round_trip_with_exact_projections(
    opaque_case: OpaqueCase,
    clickhouse_client: AsyncClient,
    clickhouse_database: str,
) -> None:
    """Round-trip every byte shape, including a three-MiB result payload."""
    async with asyncio.timeout(_SCENARIO_TIMEOUT_SECONDS):
        async with repository_actions.provisioned_repository(
            clickhouse_client,
            clickhouse_database,
            f"opaque_{opaque_case.name.replace('-', '_')}",
        ) as harness:
            await assert_opaque_round_trip(harness, opaque_case)


async def test_latest_expired_tombstone_and_equality_never_resurrect_history(
    clickhouse_client: AsyncClient,
    clickhouse_database: str,
) -> None:
    """Evaluate visibility after latest selection for results and progress."""
    async with asyncio.timeout(_SCENARIO_TIMEOUT_SECONDS):
        async with repository_actions.provisioned_repository(
            clickhouse_client,
            clickhouse_database,
            "visibility",
        ) as harness:
            now = await table_io.server_now(harness.gateway)
            result_rows = record_builders.visibility_result_records(harness.namespace, now)
            progress_rows = record_builders.visibility_progress_records(harness.namespace, now)
            await table_io.insert_result_records(harness.gateway, harness.layout, result_rows)
            await table_io.insert_progress_records(harness.gateway, harness.layout, progress_rows)

            assert await harness.repository.read_result_no_log("expired-result") is None
            assert await harness.repository.read_result_with_log("expired-result") is None
            assert not await harness.repository.is_result_ready("expired-result")
            assert await harness.repository.read_result_no_log("tombstone-result") is None
            assert not await harness.repository.is_result_ready("tombstone-result")
            assert await harness.repository.read_progress("expired-progress") is None

            equal_repository = repository_actions.repository(
                EqualObservationGateway(harness.gateway),
                harness.layout,
                harness.namespace,
            )
            assert await equal_repository.read_result_no_log("equality-result") is None
            assert not await equal_repository.is_result_ready("equality-result")


async def test_targeted_tombstone_leaves_newer_result_and_defeats_retry(
    clickhouse_client: AsyncClient,
    clickhouse_database: str,
) -> None:
    """Keep B after consuming A and keep state one above a retried result."""
    async with asyncio.timeout(_SCENARIO_TIMEOUT_SECONDS):
        async with repository_actions.provisioned_repository(
            clickhouse_client,
            clickhouse_database,
            "tombstone",
        ) as harness:
            record_a = await harness.repository.write_result("targeted", b"A", b"log-A")
            selected_a = await harness.repository.read_result_with_log("targeted")
            assert selected_a is not None
            record_b = await harness.repository.write_result("targeted", b"B", b"log-B")

            tombstone_a = await harness.repository.write_tombstone(selected_a)
            latest = await harness.repository.read_result_with_log("targeted")

            assert tombstone_a.generation_at == record_a.generation_at
            assert tombstone_a.generation_id == record_a.generation_id
            assert tombstone_a.state == TOMBSTONE_STATE
            assert latest is not None
            assert latest.generation_id == record_b.generation_id
            assert (latest.result_payload, latest.log_payload) == (b"B", b"log-B")

            retried = await harness.repository.write_result("retry", b"value", b"log")
            selected = await harness.repository.read_result_no_log("retry")
            assert selected is not None
            await harness.repository.write_tombstone(selected)
            await table_io.insert_result_records(harness.gateway, harness.layout, (retried,))

            assert await harness.repository.read_result_no_log("retry") is None
            assert not await harness.repository.is_result_ready("retry")

            fresh = await harness.repository.write_result("retry", b"fresh", b"fresh-log")
            visible = await harness.repository.read_result_with_log("retry")

            assert fresh.generation_at > retried.generation_at
            assert await harness.repository.is_result_ready("retry")
            assert visible is not None
            assert visible.generation_id == fresh.generation_id
            assert (visible.result_payload, visible.log_payload) == (b"fresh", b"fresh-log")


async def test_future_seed_and_concurrent_allocations_use_uuid_total_order(
    clickhouse_client: AsyncClient,
    clickhouse_client_factory: ClickHouseClientFactory,
    clickhouse_database: str,
) -> None:
    """Allocate above a future maximum and resolve one timestamp tie by UUID."""
    async with asyncio.timeout(_SCENARIO_TIMEOUT_SECONDS):
        async with repository_actions.provisioned_repository(
            clickhouse_client,
            clickhouse_database,
            "concurrent",
        ) as harness:
            now = await table_io.server_now(harness.gateway)
            future_generation = now + timedelta(hours=1)
            seed = record_builders.future_result_record(
                harness.namespace,
                now,
                future_generation,
            )
            await table_io.insert_result_records(harness.gateway, harness.layout, (seed,))
            repositories = await repository_actions.concurrent_repositories(
                clickhouse_client_factory,
                harness,
                _CONCURRENT_WRITERS,
            )
            payloads = tuple(f"concurrent-{number}".encode() for number in range(_CONCURRENT_WRITERS))

            records = await asyncio.gather(
                *(
                    storage_repository.write_result("same-point", payload, b"")
                    for storage_repository, payload in zip(
                        repositories,
                        payloads,
                        strict=True,
                    )
                ),
            )

            expected_generation = future_generation + timedelta(microseconds=1)
            assert {record.generation_at for record in records} == {expected_generation}
            assert len({record.generation_id for record in records}) == _CONCURRENT_WRITERS
            assert all(record.written_at < record.generation_at for record in records)
            expected_id, expected_payload = await table_io.expected_latest(
                harness.gateway,
                harness,
                "same-point",
            )
            latest = await harness.repository.read_result_no_log("same-point")
            repeated = await harness.repository.read_result_no_log("same-point")
            assert latest is not None
            assert repeated is not None
            assert latest.generation_id == expected_id
            assert latest.result_payload == expected_payload
            assert repeated.generation_id == expected_id
            assert repeated.result_payload == expected_payload
            assert expected_id in {record.generation_id for record in records}


async def test_latest_reads_cross_reversed_purge_partitions(
    clickhouse_client: AsyncClient,
    clickhouse_database: str,
) -> None:
    """Order result and progress generations independently of purge partitions."""
    async with asyncio.timeout(_SCENARIO_TIMEOUT_SECONDS):
        async with repository_actions.provisioned_repository(
            clickhouse_client,
            clickhouse_database,
            "partitions",
        ) as harness:
            now = await table_io.server_now(harness.gateway)
            result_rows = record_builders.partition_result_records(harness.namespace, now)
            progress_rows = record_builders.partition_progress_records(harness.namespace, now)
            await table_io.insert_result_records(harness.gateway, harness.layout, result_rows)
            await table_io.insert_progress_records(harness.gateway, harness.layout, progress_rows)

            result = await harness.repository.read_result_no_log("result-partitions")
            progress = await harness.repository.read_progress("progress-partitions")
            result_partitions = await table_io.active_partitions(
                harness.gateway,
                harness.layout.result_table.table.value,
                clickhouse_database,
            )
            progress_partitions = await table_io.active_partitions(
                harness.gateway,
                harness.layout.progress_table.table.value,
                clickhouse_database,
            )

            assert result is not None
            assert result.result_payload == b"newer-result"
            assert progress is not None
            assert progress.progress_payload == b"newer-progress"
            assert set(result_partitions) == {"209001", "209002"}
            assert set(progress_partitions) == {"209007", "209008"}


@pytest.mark.parametrize("loss_case", RESPONSE_LOSS_PARAMETERS)
async def test_committed_response_loss_confirms_exact_storage_identity(
    loss_case: ResponseLossCase,
    clickhouse_client: AsyncClient,
    clickhouse_database: str,
) -> None:
    """Confirm committed result, progress and tombstone writes without retry."""
    async with asyncio.timeout(_SCENARIO_TIMEOUT_SECONDS):
        async with repository_actions.provisioned_repository(
            clickhouse_client,
            clickhouse_database,
            "response_loss",
        ) as harness:
            loss_gateway = ResponseLossGateway(harness.gateway, loss_case)
            loss_repository = repository_actions.repository(
                loss_gateway,
                harness.layout,
                harness.namespace,
            )

            await loss_case.exercise(
                loss_repository,
                harness.repository,
            )

            assert loss_gateway.loss_count == 1
            assert loss_gateway.insert_count == 1
            assert loss_gateway.confirmation_count == 1
