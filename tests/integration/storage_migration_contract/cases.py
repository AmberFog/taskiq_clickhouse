"""Typed response-loss and physical-drift behavior matrices."""

from dataclasses import dataclass
from enum import StrEnum
from typing import Final

import pytest

from tests.integration.storage_migration_contract import queries


class LostResponse(StrEnum):
    """Committed ClickHouse response that a scenario deliberately loses."""

    RESULT_DDL = "result_ddl"
    PROGRESS_DDL = "progress_ddl"
    HISTORY_INSERT = "history_insert"


@dataclass(frozen=True, slots=True)
class ResponseLossScenario:
    """One ambiguous-response recovery case and its observable expectation."""

    case_id: str
    response: LostResponse
    expected_confirmation_count: int


@dataclass(frozen=True, slots=True)
class PhysicalDriftScenario:
    """Declarative mutation that makes the result table physically incompatible."""

    case_id: str
    expected_mismatch_path: str
    expected_actual: str | bool | tuple[str, ...]
    query_replacements: tuple[tuple[str, str], ...] = ()
    alter_query_template: str | None = None
    creates_dependent_view: bool = False


RESPONSE_LOSS_CASES: Final = tuple(
    pytest.param(scenario, id=scenario.case_id)
    for scenario in (
        ResponseLossScenario(
            case_id="response-lost-after-result-ddl",
            response=LostResponse.RESULT_DDL,
            expected_confirmation_count=0,
        ),
        ResponseLossScenario(
            case_id="response-lost-after-progress-ddl",
            response=LostResponse.PROGRESS_DDL,
            expected_confirmation_count=0,
        ),
        ResponseLossScenario(
            case_id="response-lost-after-history-insert",
            response=LostResponse.HISTORY_INSERT,
            expected_confirmation_count=1,
        ),
    )
)

PHYSICAL_DRIFT_CASES: Final = tuple(
    pytest.param(scenario, id=scenario.case_id)
    for scenario in (
        PhysicalDriftScenario(
            case_id="physical-drift-extra-column",
            expected_mismatch_path="columns.unexpected",
            expected_actual=("unexpected_payload",),
            alter_query_template=queries.ADD_UNEXPECTED_COLUMN,
        ),
        PhysicalDriftScenario(
            case_id="physical-drift-column-ttl",
            expected_mismatch_path="columns[8].describe.ttl_expression",
            expected_actual="purge_at",
            alter_query_template=queries.ADD_COLUMN_TTL,
        ),
        PhysicalDriftScenario(
            case_id="physical-drift-sampling-key",
            expected_mismatch_path="sampling_key",
            expected_actual="cityHash64(task_id)",
            query_replacements=(
                (
                    "PRIMARY KEY (namespace, task_id)",
                    "PRIMARY KEY (namespace, task_id, cityHash64(task_id))",
                ),
                (
                    "    task_id,\n    generation_at,",
                    "    task_id,\n    cityHash64(task_id),\n    generation_at,",
                ),
                (
                    "\nTTL purge_at DELETE",
                    "\nSAMPLE BY cityHash64(task_id)\nTTL purge_at DELETE",
                ),
            ),
        ),
        PhysicalDriftScenario(
            case_id="physical-drift-constraint",
            expected_mismatch_path="auxiliary.constraints",
            expected_actual=True,
            alter_query_template=queries.ADD_CONSTRAINT,
        ),
        PhysicalDriftScenario(
            case_id="physical-drift-data-skipping-index",
            expected_mismatch_path="auxiliary.data_skipping_indices",
            expected_actual=True,
            alter_query_template=queries.ADD_DATA_SKIPPING_INDEX,
        ),
        PhysicalDriftScenario(
            case_id="physical-drift-projection",
            expected_mismatch_path="auxiliary.projections",
            expected_actual=True,
            alter_query_template=queries.ADD_PROJECTION,
        ),
        PhysicalDriftScenario(
            case_id="physical-drift-materialized-view",
            expected_mismatch_path="auxiliary.materialized_views",
            expected_actual=True,
            creates_dependent_view=True,
        ),
    )
)
