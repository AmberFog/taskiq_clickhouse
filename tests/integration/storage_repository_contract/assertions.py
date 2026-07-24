"""Observable assertions for opaque storage repository projections."""

# ruff: noqa: S101

from tests.integration.storage_repository_contract.cases import OpaqueCase
from tests.integration.storage_repository_contract.gateways import QueryCall
from tests.integration.storage_repository_contract.repository_actions import (
    RepositoryHarness,
)


async def assert_opaque_round_trip(
    harness: RepositoryHarness,
    opaque_case: OpaqueCase,
) -> None:
    """Assert byte preservation and the exact payload projection contract."""
    task_id = f"opaque-{opaque_case.name}"
    await harness.repository.write_result(
        task_id,
        opaque_case.result_payload,
        opaque_case.log_payload,
    )
    await harness.repository.write_progress(task_id, opaque_case.progress_payload)

    ready = await harness.repository.is_result_ready(task_id)
    readiness_call = harness.projection_gateway.query_calls[-1]
    no_log = await harness.repository.read_result_no_log(task_id)
    no_log_call = harness.projection_gateway.query_calls[-1]
    with_log = await harness.repository.read_result_with_log(task_id)
    with_log_call = harness.projection_gateway.query_calls[-1]
    progress = await harness.repository.read_progress(task_id)
    progress_call = harness.projection_gateway.query_calls[-1]

    assert ready
    assert no_log is not None
    assert (no_log.result_payload, no_log.log_payload) == (
        opaque_case.result_payload,
        None,
    )
    assert with_log is not None
    assert (with_log.result_payload, with_log.log_payload) == (
        opaque_case.result_payload,
        opaque_case.log_payload,
    )
    assert progress is not None
    assert progress.progress_payload == opaque_case.progress_payload
    _assert_projection_calls(
        readiness_call,
        no_log_call,
        with_log_call,
        progress_call,
    )


def _assert_projection_calls(
    readiness: QueryCall,
    no_log: QueryCall,
    with_log: QueryCall,
    progress: QueryCall,
) -> None:
    assert "result_payload" not in readiness.query
    assert "log_payload" not in readiness.query
    assert readiness.column_formats == {}
    assert "result_payload" in no_log.query
    assert "log_payload" not in no_log.query
    assert no_log.column_formats == {"result_payload": "bytes"}
    assert "result_payload" in with_log.query
    assert "log_payload" in with_log.query
    assert with_log.column_formats == {
        "result_payload": "bytes",
        "log_payload": "bytes",
    }
    assert "progress_payload" in progress.query
    assert progress.column_formats == {"progress_payload": "bytes"}
