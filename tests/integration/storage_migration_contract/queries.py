"""Exact ClickHouse probes and physical-drift mutations for migration tests."""

from typing import Final


DROP_TABLE: Final = "DROP TABLE IF EXISTS {table} SYNC"
ADD_UNEXPECTED_COLUMN: Final = "ALTER TABLE {table} ADD COLUMN `unexpected_payload` String"
ADD_COLUMN_TTL: Final = "ALTER TABLE {table} MODIFY COLUMN `result_payload` String TTL purge_at"
ADD_CONSTRAINT: Final = "ALTER TABLE {table} ADD CONSTRAINT reject_inserts CHECK 0"
ADD_DATA_SKIPPING_INDEX: Final = "ALTER TABLE {table} ADD INDEX exactness_state_idx state TYPE minmax GRANULARITY 1"
ADD_PROJECTION: Final = (
    "ALTER TABLE {table} ADD PROJECTION exactness_task_projection "
    "(SELECT namespace, task_id ORDER BY (namespace, task_id))"
)
CREATE_MATERIALIZED_VIEW_TARGET: Final = """
CREATE TABLE {target}
(
    namespace String,
    task_id String,
    CONSTRAINT reject_writes CHECK 0
)
ENGINE = MergeTree
ORDER BY (namespace, task_id)
"""
CREATE_DEPENDENT_MATERIALIZED_VIEW: Final = """
CREATE MATERIALIZED VIEW {view}
TO {target}
AS SELECT namespace, task_id FROM {source}
"""
PROBE_MATERIALIZED_VIEW_INSERT: Final = """
INSERT INTO {source} (namespace, task_id)
VALUES ('materialized-view-probe', 'materialized-view-probe')
"""
TRUNCATE_TABLE: Final = "TRUNCATE TABLE {table}"
COUNT_ROWS: Final = "SELECT count() FROM {table}"
COUNT_METADATA: Final = """
SELECT count(), uniqExact(attempt_id)
FROM {table}
WHERE record_kind = {{record_kind:String}}
  AND scope = {{scope:String}}
  AND record_key = {{record_key:String}}
"""
CONFIRMATION_MARKER: Final = "attempt_id = {attempt_id:UUID}"
