"""Project raw schema comparisons into neutral value-free diagnostics."""

from taskiq_clickhouse._schema._inspection_diff_types import (
    SchemaDifference,
    SchemaMismatch,
)
from taskiq_clickhouse._schema_drift import SchemaDriftLocation, SchemaDriftReport


def safe_drift_report(difference: SchemaDifference) -> SchemaDriftReport:
    """Discard physical values while preserving every mismatch coordinate."""
    if type(difference) is not SchemaDifference:  # noqa: WPS516 - reject forged internal comparison containers.
        msg = "difference must be a SchemaDifference"
        raise TypeError(msg)
    locations = tuple(_safe_drift_location(mismatch) for mismatch in difference.mismatches)
    return SchemaDriftReport(
        mismatch_count=len(difference.mismatches),
        locations=locations,
    )


def _safe_drift_location(mismatch: object) -> SchemaDriftLocation:
    if type(mismatch) is not SchemaMismatch:  # noqa: WPS516 - raw values must use the package comparison DTO.
        msg = "difference mismatches must be SchemaMismatch values"
        raise TypeError(msg)
    return SchemaDriftLocation(table=mismatch.table, path=mismatch.path)
