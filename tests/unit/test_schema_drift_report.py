"""Verify value-free physical-schema drift diagnostics."""

from typing import cast

import pytest

from taskiq_clickhouse._identifiers import Identifier, QualifiedTable
from taskiq_clickhouse._schema._drift_projection import safe_drift_report
from taskiq_clickhouse._schema._inspection_diff_types import SchemaDifference, SchemaMismatch
from taskiq_clickhouse._schema_drift import (
    SchemaDriftLocation,
    SchemaDriftReport,
)


TABLE = QualifiedTable(Identifier("tasks"), Identifier("results"))


def test_drift_report_retains_only_count_and_locations() -> None:
    """Represent diagnostics without slots for expected or actual values."""
    report = SchemaDriftReport(
        mismatch_count=2,
        locations=(
            SchemaDriftLocation(TABLE, "engine"),
            SchemaDriftLocation(TABLE, "columns[0].describe.comment"),
        ),
    )
    assert report == SchemaDriftReport(
        mismatch_count=2,
        locations=(
            SchemaDriftLocation(TABLE, "engine"),
            SchemaDriftLocation(TABLE, "columns[0].describe.comment"),
        ),
    )
    assert not hasattr(report, "expected")
    assert not hasattr(report, "actual")


@pytest.mark.parametrize(
    ("table", "path", "error_type"),
    [
        (object(), "engine", TypeError),
        (TABLE, object(), TypeError),
        (TABLE, "password=unsafe-path", ValueError),
    ],
)
def test_drift_location_rejects_unsafe_coordinates(
    table: object,
    path: object,
    error_type: type[Exception],
) -> None:
    """Accept neither foreign value objects nor arbitrary diagnostic text."""
    with pytest.raises(error_type):
        SchemaDriftLocation(
            cast("QualifiedTable", table),
            cast("str", path),
        )


@pytest.mark.parametrize(
    ("mismatch_count", "locations", "error_type"),
    [
        (True, (), TypeError),
        (0, (), ValueError),
        (1, [], TypeError),
        (1, (object(),), TypeError),
        (2, (SchemaDriftLocation(TABLE, "engine"),), ValueError),
    ],
)
def test_drift_report_rejects_incoherent_shapes(
    mismatch_count: object,
    locations: object,
    error_type: type[Exception],
) -> None:
    """Keep count and immutable coordinates as one exact invariant."""
    with pytest.raises(error_type):
        SchemaDriftReport(
            cast("int", mismatch_count),
            cast("tuple[SchemaDriftLocation, ...]", locations),
        )


def test_drift_projection_rejects_foreign_difference_container() -> None:
    """Accept raw comparison details only through the exact schema DTO."""
    with pytest.raises(TypeError, match="must be a SchemaDifference"):
        safe_drift_report(cast("SchemaDifference", object()))


def test_drift_projection_rejects_foreign_mismatch_value() -> None:
    """Never inspect coordinates from a forged raw mismatch object."""
    difference = SchemaDifference(
        mismatches=(cast("SchemaMismatch", object()),),
    )

    with pytest.raises(TypeError, match="must be SchemaMismatch"):
        safe_drift_report(difference)
