# Operator guide

This runbook covers the v0.1 single-endpoint deployment contract. It assumes
the configured ClickHouse database already exists and that security
administrators create users or roles separately.

Examples use these default objects; substitute the exact validated names from
the application factory everywhere:

```text
database:       application
metadata table: taskiq_clickhouse_metadata
result table:   taskiq_clickhouse_results
progress table: taskiq_clickhouse_progress
```

## Deployment boundary

The configured host and port must expose one logical service with a coherent
catalog, server time, read-after-DDL visibility, and read-after-write visibility.
Keep that service's clocks synchronized. v0.1 does not:

- create the database;
- emit `ON CLUSTER`;
- create `Distributed`, replicated, or sharded tables;
- coordinate DDL across nodes;
- own replication or load-balancer configuration; or
- guarantee cross-replica read-after-write behavior.

A load balancer that can route one backend instance across divergent catalogs
or replica visibility is outside this contract. Successful connection or
startup against one node does not establish cluster support.

The same synchronized-clock assumption applies after physical TTL cleanup.
Purge-deadline floors order eligibility, but they are not a durable per-task
watermark: if ClickHouse removes a newer suppressing part while an older part
lags, a later clock rollback can expose that retained row. Avoid backward clock
jumps across expired visibility windows; v0.1 does not guarantee recovery from
that sequence.

## Controlled schema manager

The public in-process manager is single-use and accepts only a fresh backend:

```python
from taskiq_clickhouse.schema import ClickHouseSchemaManager

backend = make_result_backend()  # synchronous, side-effect-free, and NEW
await ClickHouseSchemaManager(backend).migrate()
```

`migrate()` uses a temporary client, applies allowed AUTO and CONTROLLED
migrations, crosses the full schema/namespace barrier, closes the client, and
leaves the backend `NEW`. `validate()` performs the same read barrier without
DDL or metadata writes. A manager object cannot be reused after either method
starts.

The installed console command loads a trusted synchronous zero-argument
factory. The factory must return a new `ClickHouseResultBackend`; it may read
credentials from the application's secret/configuration system, keeping them
out of process arguments:

```console
taskiq-clickhouse-schema migrate your_package.taskiq_app:make_result_backend
taskiq-clickhouse-schema validate your_package.taskiq_app:make_result_backend
```

The manager command overrides the factory backend's configured `schema_mode`
for that invocation. Exit status is `0` on success, `1` for a sanitized
operational/schema failure, and `2` for command, import, factory, or
configuration usage errors. Treat the imported factory/module as trusted code.

The copyable factory in [`examples/taskiq_app.py`](examples/taskiq_app.py) is
also attached to a Taskiq broker.

## Privilege matrix

The following matrix is the current migration-v1 SQL capability set, not a
generic ClickHouse role recommendation. The validation read set is exercised by
the integration access-control suite. DDL and metadata privileges follow the
exact current `CREATE TABLE IF NOT EXISTS` and native `INSERT` operations.

| Capability | Schema `validate` | Schema `migrate` | READY data worker |
| --- | --- | --- | --- |
| Connect/basic probe | `SELECT system.one` | same | same |
| Inspect tables/columns | `SELECT system.tables`, `system.columns`, `system.data_skipping_indices`, `system.projections`; `SHOW COLUMNS` on all three managed tables | same | same at startup |
| Read migration/namespace registry | `SELECT` metadata table | same | same at startup |
| Bootstrap current schema | none | `CREATE TABLE` on metadata, result, and progress tables | only when worker `schema_mode="migrate"` may run migration |
| Record migration/namespace | none | `INSERT` metadata table | only in worker migrate mode when a record is missing |
| Read/allocate/confirm result and progress | none for manager-only validate | none for manager-only migrate | `SELECT` result and progress tables |
| Write results, progress, tombstones | none | none | `INSERT` result and progress tables |

`schema_mode="validate"` lets normal workers avoid metadata `INSERT` and every
DDL grant after controlled pre-provisioning. A worker that actually reads or
writes Taskiq data still needs the data-table grants in the last two rows.

### Copyable grants for a validate-only schema principal

Assume security administration has created `taskiq_validate`:

```sql
GRANT SELECT ON `system`.`one` TO `taskiq_validate`;
GRANT SELECT ON `system`.`tables` TO `taskiq_validate`;
GRANT SELECT ON `system`.`columns` TO `taskiq_validate`;
GRANT SELECT ON `system`.`data_skipping_indices` TO `taskiq_validate`;
GRANT SELECT ON `system`.`projections` TO `taskiq_validate`;

GRANT SHOW COLUMNS ON `application`.`taskiq_clickhouse_metadata` TO `taskiq_validate`;
GRANT SHOW COLUMNS ON `application`.`taskiq_clickhouse_results` TO `taskiq_validate`;
GRANT SHOW COLUMNS ON `application`.`taskiq_clickhouse_progress` TO `taskiq_validate`;

GRANT SELECT ON `application`.`taskiq_clickhouse_metadata` TO `taskiq_validate`;
```

These grants are enough for the schema CLI `validate` operation. They do not
make the principal a usable result worker.

### Additional current-v1 migrator grants

Assume the migrator also has all validate grants:

```sql
GRANT CREATE TABLE ON `application`.`taskiq_clickhouse_metadata` TO `taskiq_migrator`;
GRANT CREATE TABLE ON `application`.`taskiq_clickhouse_results` TO `taskiq_migrator`;
GRANT CREATE TABLE ON `application`.`taskiq_clickhouse_progress` TO `taskiq_migrator`;

GRANT INSERT ON `application`.`taskiq_clickhouse_metadata` TO `taskiq_migrator`;
```

Grant the validate set to `taskiq_migrator` as well, either directly or through
your role system. Future package migrations can require a different DDL
privilege; inspect their release migration descriptor before granting it. Do
not infer an `ALTER`, `DROP`, mutation, or database-creation requirement for
the current v1 migration: it issues none.

### Additional data-worker grants

Assume a normal `schema_mode="validate"` worker has all validate grants:

```sql
GRANT SELECT ON `application`.`taskiq_clickhouse_results` TO `taskiq_worker`;
GRANT INSERT ON `application`.`taskiq_clickhouse_results` TO `taskiq_worker`;
GRANT SELECT ON `application`.`taskiq_clickhouse_progress` TO `taskiq_worker`;
GRANT INSERT ON `application`.`taskiq_clickhouse_progress` TO `taskiq_worker`;
```

Result `SELECT` covers readiness, allocation, retrieval, and exact-write
confirmation. Result `INSERT` covers ordinary result rows and consume
tombstones. The progress pair covers allocation, reads, writes, and
confirmation. The backend never converts permission denial into “not ready,”
“not found,” or missing progress.

Monitoring queries later in this guide require separate operator access to
additional `system` tables. Do not add monitoring privileges to workers merely
to run the backend.

## Current v1 physical layout

The schema manager, not application deployment SQL, owns these statements. The
following default-name rendering is included so operators can audit physical
postconditions; substitute configured identifiers when comparing catalogs.
Production statements are shipped as package `.sql` resources and loaded once
with `pathlib`; they are not discovered relative to the process working
directory. Values and validated database/table names use typed ClickHouse
parameters. Column definitions, engine, keys, TTL clauses, and other SQL
structure remain fixed, while row payloads continue to use native inserts.

```sql
CREATE TABLE IF NOT EXISTS `application`.`taskiq_clickhouse_metadata`
(
    `record_kind` String,
    `scope` String,
    `record_key` String,
    `version` UInt32,
    `name` String,
    `payload` String,
    `checksum` String,
    `package_version` String,
    `recorded_at` DateTime64(6, 'UTC'),
    `attempt_id` UUID
)
ENGINE = MergeTree
PRIMARY KEY (record_kind, scope, record_key, version)
ORDER BY
(
    record_kind,
    scope,
    record_key,
    version,
    checksum,
    recorded_at,
    attempt_id
);
```

```sql
CREATE TABLE IF NOT EXISTS `application`.`taskiq_clickhouse_results`
(
    `namespace` String,
    `task_id` String,
    `generation_at` DateTime64(6, 'UTC'),
    `generation_id` UUID,
    `state` UInt8,
    `written_at` DateTime64(6, 'UTC'),
    `visible_until` DateTime64(6, 'UTC'),
    `purge_at` DateTime64(6, 'UTC'),
    `result_payload` String,
    `log_payload` String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(purge_at)
PRIMARY KEY (namespace, task_id)
ORDER BY
(
    namespace,
    task_id,
    generation_at,
    generation_id,
    state
)
TTL purge_at DELETE;
```

```sql
CREATE TABLE IF NOT EXISTS `application`.`taskiq_clickhouse_progress`
(
    `namespace` String,
    `task_id` String,
    `generation_at` DateTime64(6, 'UTC'),
    `generation_id` UUID,
    `written_at` DateTime64(6, 'UTC'),
    `visible_until` DateTime64(6, 'UTC'),
    `purge_at` DateTime64(6, 'UTC'),
    `progress_payload` String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(purge_at)
PRIMARY KEY (namespace, task_id)
ORDER BY
(
    namespace,
    task_id,
    generation_at,
    generation_id
)
TTL purge_at DELETE;
```

There are no declared column defaults, codecs, comments, or column TTLs. The
metadata table has no partition or table TTL. No constraint, data-skipping
index, projection, or dependent materialized view is part of the contract.
In the result table, `state=0` is a result and `state=1` is a generation-targeted
tombstone; every other value is corruption.

## What startup validates

Every worker startup and manager invocation reads permanent history and
physically inspects all managed tables. A migration checksum never substitutes
for physical validation.

For each required-present or required-absent table, the inspector reads
parameterized projections from `system.tables`, `system.columns`, and
`DESCRIBE TABLE ... SETTINGS describe_include_subcolumns = 0`. It compares:

- table presence for the current migration phase;
- exact `MergeTree` engine;
- normalized partition, sorting, primary, and sampling-key expressions;
- the table-level delete TTL (`purge_at` for result/progress; none for
  metadata);
- exact ordered column names, positions, types, and counts from both catalog
  surfaces;
- column default kind/expression, compression codec, comment, and absence of
  column TTL;
- declared critical MergeTree settings; current migration v1 declares no
  package-tuned critical setting;
- absence of top-level constraints;
- absence of data-skipping indexes;
- absence of projections; and
- absence of dependent materialized views.

Constraint detection parses the server's `formatQuery(create_table_query)`
shape and checks a known constraint probe so an unsupported formatter shape
fails closed. Indexes and projections come from their stable system catalogs;
dependent materialized-view presence comes from the table dependency fields.
Malformed row shapes, non-binary presence flags, and unavailable catalog
capabilities fail the barrier instead of silently skipping a check.

Constraints, data-skipping indexes, projections, and dependent materialized
views are forbidden on all three managed v0.1 tables, even if an operator
believes one is harmless. A dependent materialized view is particularly unsafe
for acknowledgement: ClickHouse can commit the source insert before a view
target rejects it, leaving a partial outcome.

Current result, progress, and metadata contracts allow no unexpected columns.
The comparison does not require raw `SHOW CREATE TABLE` string equality and
does not treat server UUID or storage-policy placement as package schema. It
also does not validate arbitrary undeclared `engine_full` settings; the
observed default `index_granularity=8192` is not a package tuning promise.

Physical drift is reported as `ClickHouseSchemaDriftError`. The CLI prints only
a bounded count and safe table/path coordinates, never expected/actual raw
catalog values.

## Registry and migration guarantees

`taskiq_clickhouse_metadata` is a permanent, append-only `MergeTree` with no
TTL. It stores migration evidence and namespace contracts. Successful migration
records are appended only after DDL postconditions pass.

Migration definitions are ordered, checksummed, and forward-only. Current
migration v1 creates the result and progress tables and is `AUTO`, re-entrant,
and concurrent-safe. Worker `schema_mode="migrate"` may apply only `AUTO`
migrations with both safety flags. A future `CONTROLLED` migration is rejected
by workers and requires `ClickHouseSchemaManager.migrate()` plus operator-owned
drain/exclusivity.

The registry is evidence, not a distributed lock. Identical concurrent
migration/namespace records are tolerated; conflicting checksums or contracts
fail closed when observed. There is no transactional compare-and-set, leader
election, or exactly-once DDL claim. One worker can become ready just before a
conflicting first registration appears, and an already-ready worker is not
asynchronously revoked. Strict exclusion requires controlled pre-provisioning
or an external coordinator.

The namespace contract is scoped by the qualified result/progress table pair
and namespace. It permanently fixes:

- serializer id;
- `taskiq-pydantic2-python-v1` payload format;
- result and progress table names; and
- result/purge TTLs as integer microseconds.

An apparently empty table is not evidence that this contract can be changed:
ClickHouse TTL cleanup is asynchronous and metadata is permanent. Use a new
namespace/table pair or an explicitly designed migration.

## Rollout

### Recommended initial deployment

1. Create the database outside this package.
2. Create a trusted migrator and validate-only/data-worker principals with the
   exact configured table names.
3. Run `taskiq-clickhouse-schema migrate module:factory` with the candidate
   package and migrator credentials.
4. Run `taskiq-clickhouse-schema validate module:factory` using worker-equivalent
   schema credentials.
5. Start workers with `schema_mode="validate"`.
6. Verify startup error rate, active parts, TTL lag, merge pressure, and insert
   failures using the signals below.

This sequence prevents workers from racing a first namespace registration and
removes DDL/metadata-write privileges from the steady-state data role.

### Additive rolling rollout

For a release whose migration notes explicitly classify every schema change as
backward-compatible and AUTO/additive:

1. Pre-provision with the new package's controlled migrator.
2. Validate with the new package and the intended worker configuration.
3. Roll workers gradually while watching failures and lag.
4. Remember that an old package restarted after newer migration history appears
   fails closed with `newer_version`; only already-ready compatible old workers
   can remain during the planned overlap.

Do not call a change “additive” merely because ClickHouse accepted its DDL. It
must be declared compatible by the package schema/migration contract.

### Breaking or CONTROLLED rollout

1. Stop result/progress producers and new task admission.
2. Let every in-flight backend operation quiesce, then stop all old workers.
3. Establish the exclusivity required by the migration release notes.
4. Back up managed tables and permanent metadata under your normal recovery
   policy.
5. Run the new package's controlled migrator, then validate.
6. Deploy and start only the compatible worker version.
7. Resume admission after readiness and operational checks pass.

The package does not drain Taskiq tasks, revoke already-ready workers, take a
distributed lock, or roll back a breaking migration for the operator.

## Recovery

### Broker acknowledgement after final-result persistence failure

Taskiq 0.12.4's stock receiver is not a durable-result gate: after
result-backend `set_result()` fails it logs the error and still reaches the
`when_saved` RabbitMQ ACK. The backend correctly reports failure and must not
own the later broker operation.

Use the separately exported `ResultPersistenceReceiver` through Taskiq's
supported CLI receiver extension point:

```console
taskiq worker your_package.taskiq_app:broker \
  --receiver taskiq_clickhouse:ResultPersistenceReceiver \
  --ack-type when_saved \
  --workers 1 \
  --max-fails 1 \
  --max-async-tasks 1 \
  --max-async-tasks-jitter 0 \
  --max-prefetch 1 \
  --max-threadpool-threads 1 \
  --shutdown-timeout 30 \
  --wait-tasks-timeout 30
```

This is the only qualified v0.1 worker entry point for result-gated RabbitMQ
ACK. The receiver fails construction unless ACK mode is `when_saved`,
`max_async_tasks=1`, `max_async_tasks_jitter=0`, `max_prefetch=1`, and a finite
positive `wait_tasks_timeout` are present. It defers the original
`AckableMessage.ack` call until Taskiq's complete save phase returns. On
save-phase failure it leaves the delivery unacknowledged, closes admission for
any already prefetched sibling callback, and signals the consumer to stop.
Awaitable ACK is also bounded by `wait_tasks_timeout`; an exception,
cancellation, or timeout after ACK begins is an ambiguous settlement, so the
receiver stops and never retries ACK in-process.

These limits make a persistence-triggered stop locally deterministic; they do
not time-limit application code, synchronous thread-pool work, dependency or
middleware code before the failure, or OS process termination. Backend driver
timeouts continue to bound ClickHouse I/O, and the external supervisor must
own a finite hard-kill budget. Taskiq remains the broker lifecycle owner.
Closing the RabbitMQ channel releases unacknowledged deliveries for
broker-controlled redelivery.
The receiver instance is single-use; Taskiq's CLI constructs one for each
worker child. Reusing an instance or embedding a manual receiver lifecycle is
outside the qualified path.

Configure the task queue as quorum with a positive delivery limit and a
separate dead-letter queue. The core distribution does not install a broker
transport. Install `taskiq-aio-pika==0.6.0` separately for the qualified
RabbitMQ 4.3.2 path. The relevant topology is explicit:

```python
from taskiq_aio_pika import AioPikaBroker
from taskiq_aio_pika.queue import Queue, QueueType


task_queue = Queue(
    name="taskiq.results",
    type=QueueType.QUORUM,
    arguments={"x-delivery-limit": 3},
)
dead_letter_queue = Queue(
    name="taskiq.results.dead",
    type=QueueType.QUORUM,
)
broker = AioPikaBroker(
    qos=1,
    task_queues=[task_queue],
    dead_letter_queue=dead_letter_queue,
).with_result_backend(result_backend)
```

`qos=1` is part of the qualification: it prevents one receiver channel from
owning a batch of broker deliveries whose delivery counters would all advance
when the channel closes. Queue type and declaration arguments are durable
topology. Reconcile an existing incompatible queue through an operator-reviewed
migration rather than expecting the worker to mutate it in place. Monitor
worker-child exits, ready/unacknowledged depth, redeliveries, and dead-letter
depth.

The demonstrated ClickHouse-outage recovery is controlled, not automatic:

1. the task may complete application effects, but final-result persistence
   fails;
2. the receiver withholds ACK, closes local admission, and the worker child
   exits; `--max-fails 1` prevents the qualified parent from replacing it;
3. quiesce every competing worker process and replica, verify that the delivery
   is ready or in the DLQ, and restore ClickHouse;
4. start one replacement consumer; a ready delivery executes again, persists
   the result, and is acknowledged; and
5. if the delivery already reached the DLQ, inspect idempotency and replay it
   through the application-owned operational procedure.

Do not rely on an ordinary process supervisor restart loop for outage recovery.
Another child or replica can consume the ready delivery immediately, fail
again, and exhaust the finite delivery budget before ClickHouse recovers;
RabbitMQ provides no retry delay in this topology. If persistence keeps
failing, quorum delivery counting routes the message to the dead-letter queue.
That bounded DLQ outcome is required and is the only guaranteed fleet-wide
failure result. A classic queue or unbounded requeue policy is not a qualified
substitute.

This contract is at-least-once. The first execution may already have committed
irreversible application effects, and redelivery can execute them again. Make
tasks idempotent or use an application-owned deduplication key; do not claim
exactly-once behavior and do not use the backend's result generation as a
transaction for application effects. Also do not substitute `when_received`,
`when_executed`, the stock receiver, a non-ackable broker, or a manually
embedded receiver lifecycle.

Before rollout, validate the transient and poison paths in the deployment-owned
broker and worker environment. Qualification must show no broker ACK after a
forced persistence failure, controlled single-consumer replay after recovery,
and dead-lettering within the configured finite delivery budget. This does not
qualify an automatic multi-worker restart policy.

### Lost response or interrupted AUTO migration

Rerun the same package version's `migrate` command. Each current step first
checks complete before/after states. Re-entrant `IF NOT EXISTS` DDL is followed
by physical postcondition validation; an ambiguous DDL response succeeds only
when the after-state is present. Migration success is recorded last.

If neither complete before-state nor after-state matches, startup reports
partial state/drift. It does not guess a repair.

### Physical drift or partial state

1. Stop rollout; for write-path drift, stop producers and quiesce workers.
2. Run the sanitized CLI `validate` command and record its table/path
   coordinates.
3. Inspect `system.tables`, `system.columns`, `DESCRIBE`, and the auxiliary
   catalogs with trusted operator credentials.
4. Compare against the package's current migration definition and a trusted
   backup/change record.
5. Restore the exact expected physical state through an operator-reviewed
   change or restore procedure.
6. Rerun `validate`, then restart workers.

Do not “fix” drift by inserting/deleting migration rows or editing checksums.
The package deliberately does not auto-repair unknown columns, keys, TTLs,
constraints, indexes, projections, or materialized views.

### Migration definition conflict or newer database version

- `definition_conflict` or `definition_changed`: stop and investigate forged,
  manually edited, or mixed package migration evidence. Restore trusted
  metadata/schema together; do not choose a checksum by row count.
- `newer_version`: deploy the package version that owns that history or a newer
  compatible release. An older process cannot downgrade the database.
- `version_gap`: restore a coherent contiguous history from trusted evidence.
  Never synthesize missing rows without the corresponding verified schema.

There is no automated down migration.

### Namespace conflict

Compare every worker/factory's table names, namespace, TTLs, serializer, and
serializer id. Drain misconfigured workers and restore one application-owned
configuration. Because permanent metadata has no compare-and-set and ready
workers are not revoked, a strict first registration should be performed by the
controlled migrator while workers are stopped.

Do not delete the old namespace row to reuse a namespace. Choose a new
namespace/table scope unless an explicit migration and data recovery plan owns
the old payloads.

## Operational signals

v0.1 has no built-in metrics endpoint. Collect application counts for public
safe exception codes and use ClickHouse operator queries for storage state.
Avoid selecting payload, log, raw query, exception, or stack-trace columns into
general monitoring systems.

### Active parts and stored rows

```sql
SELECT
    table,
    count() AS active_parts,
    sum(rows) AS stored_rows,
    formatReadableSize(sum(bytes_on_disk)) AS stored_bytes
FROM system.parts
WHERE database = 'application'
  AND table IN (
      'taskiq_clickhouse_results',
      'taskiq_clickhouse_progress',
      'taskiq_clickhouse_metadata'
  )
  AND active
GROUP BY table
ORDER BY table;
```

Alert on sustained part-count growth relative to your measured baseline, not a
universal threshold. Small append-only writes can create merge pressure.

### Logical-to-physical TTL lag

This query scans rows that have passed their earliest purge time. Schedule it
at a frequency and scope appropriate for table size:

```sql
SELECT
    'taskiq_clickhouse_results' AS table,
    count() AS rows_past_purge_at,
    minOrNull(purge_at) AS oldest_purge_at,
    dateDiff('second', minOrNull(purge_at), now64(6, 'UTC')) AS oldest_lag_seconds
FROM `application`.`taskiq_clickhouse_results`
WHERE purge_at <= now64(6, 'UTC')
UNION ALL
SELECT
    'taskiq_clickhouse_progress' AS table,
    count() AS rows_past_purge_at,
    minOrNull(purge_at) AS oldest_purge_at,
    dateDiff('second', minOrNull(purge_at), now64(6, 'UTC')) AS oldest_lag_seconds
FROM `application`.`taskiq_clickhouse_progress`
WHERE purge_at <= now64(6, 'UTC');
```

Nonzero rows are not automatically a fault: TTL deletion is merge-driven and
eventual. Alert on sustained lag together with parts/merge pressure and disk
growth. Do not use `purge_at` as logical result availability; the API uses
exclusive `visible_until` only after latest-row selection.

### Merge pressure

```sql
SELECT
    table,
    count() AS running_merges,
    sum(num_parts) AS source_parts,
    round(max(elapsed), 3) AS oldest_running_seconds,
    formatReadableSize(sum(total_size_bytes_compressed)) AS compressed_input
FROM system.merges
WHERE database = 'application'
  AND table IN (
      'taskiq_clickhouse_results',
      'taskiq_clickhouse_progress',
      'taskiq_clickhouse_metadata'
  )
GROUP BY table
ORDER BY table;
```

Combine this with active-parts and disk metrics. This guide does not recommend
routine `OPTIMIZE ... FINAL`; it can be expensive and is not required for
correct latest-result reads.

### Unexpected asynchronous-insert queue

Package result/progress/metadata inserts force `async_insert=0`, so they should
not contribute entries to `system.asynchronous_inserts`. A nonempty row for the
managed tables indicates another writer or an unexpected server/client path:

```sql
SELECT
    database,
    table,
    count() AS queued_batches,
    sum(total_bytes) AS queued_bytes,
    min(first_update) AS oldest_batch
FROM system.asynchronous_inserts
WHERE database = 'application'
  AND table IN (
      'taskiq_clickhouse_results',
      'taskiq_clickhouse_progress',
      'taskiq_clickhouse_metadata'
  )
GROUP BY database, table
ORDER BY table;
```

Do not enable async insert or `wait_for_async_insert=0` for package writes. The
constructor exposes no settings override because fire-and-forget queue
acceptance is not result acknowledgement.

### Recent package insert failures

If query logging is enabled, an operator can flush and aggregate code-only
failure facts without exporting query text or exception details:

```sql
SYSTEM FLUSH LOGS;

SELECT
    toStartOfMinute(event_time) AS minute,
    type,
    exception_code,
    count() AS failures
FROM system.query_log
WHERE event_time >= now() - INTERVAL 15 MINUTE
  AND client_name LIKE 'taskiq-clickhouse/%'
  AND query_kind = 'Insert'
  AND type IN ('ExceptionBeforeStart', 'ExceptionWhileProcessing')
GROUP BY minute, type, exception_code
ORDER BY minute DESC, type, exception_code;
```

`SYSTEM FLUSH LOGS` and `system.query_log` access are operator privileges, not
backend grants. Avoid selecting `query`, `exception`, or `stack_trace`: they can
contain deployment details.

### Permanent metadata growth and conflicts

Metadata has no TTL and grows with new scopes/namespaces, package migrations,
bounded concurrent duplicate registrations, and ambiguous-write duplicates:

```sql
SELECT
    record_kind,
    scope,
    record_key,
    version,
    count() AS physical_rows,
    uniqExact(attempt_id) AS attempts,
    min(recorded_at) AS first_recorded_at,
    max(recorded_at) AS last_recorded_at
FROM `application`.`taskiq_clickhouse_metadata`
GROUP BY record_kind, scope, record_key, version
ORDER BY record_kind, scope, record_key, version;
```

Detect conflicting checksums without selecting canonical payload text:

```sql
SELECT
    record_kind,
    scope,
    record_key,
    version,
    uniqExact(checksum) AS checksum_count,
    count() AS physical_rows
FROM `application`.`taskiq_clickhouse_metadata`
GROUP BY record_kind, scope, record_key, version
HAVING checksum_count > 1
ORDER BY record_kind, scope, record_key, version;
```

Duplicate rows with one checksum can be valid concurrency/response-loss
evidence. Multiple checksums are a fail-closed conflict; do not delete one side
automatically.

## Capacity qualification

There is no package-level latency or throughput SLO. Before rollout, qualify a
representative workload against the intended retained-history depth, payload
sizes, concurrency, TTL lag, hardware, and ClickHouse settings. Performance
diagnostics are deployment-owned and intentionally excluded from package CI.

## Troubleshooting

| Signal or safe reason | Interpretation and next action |
| --- | --- |
| `backend:not_ready` | Broker/backend startup did not complete. Start through Taskiq or call direct `startup()` before data methods. |
| `backend:foreign_runtime` | A started backend crossed PID/event-loop ownership. Construct before fork or create/start one instance inside each process and loop. |
| `backend:closed` | The instance is terminal. Build a new backend; v0.1 does not restart closed instances. |
| `backend:client_create_failed` | Authentication, TLS, endpoint, or connection creation failed. Check server-side logs and secret-store values with privileged tooling; public errors intentionally omit details. |
| `schema_inspection:database_error` | The server denied or failed a catalog/DESCRIBE read. Compare the validate grant set and endpoint catalog. |
| `schema_barrier:invalid_response` | A required catalog/formatter capability or response shape was malformed/unsupported. Verify the ClickHouse version and the single-endpoint target; do not bypass inspection. |
| `schema_validation:physical_drift` | Use CLI coordinates and the physical-drift recovery procedure; do not let runtime inserts discover compatibility. |
| `migration_barrier:migration_missing` | `validate` found incomplete target history. Run the matching controlled migrator. |
| `migration_policy:controlled_pending` | Workers cannot run a CONTROLLED migration. Drain and use the schema manager under its release preconditions. |
| `namespace_validate:contract_missing` | Validate mode was used before namespace pre-provisioning. Run controlled migrate with the identical factory configuration. |
| `namespace_validate:contract_conflict` | Serializer, TTL, payload format, namespace, or table scope differs. Reconcile all factories; do not reuse the old namespace. |
| `result_read:not_found` | No row, expired latest row, or consumed latest generation. This is intentionally one public absence category. |
| `*:ambiguous_response` or `*:write_unconfirmed` | The package could not prove the server outcome after bounded confirmation. Do not assume rollback; a new public write creates a new generation. |
| encode/decode safe code | Verify JSON graph support and the custom serializer version. Calls are serialized within one backend, but a serializer object shared across backends must be thread-safe. A custom serializer is never auto-detected or replaced. |
| Encode/decode calls queue behind slow hooks | One backend admits one serializer call at a time; the shared process pool admits at most 32 submitted jobs. Reduce application concurrency or fix the hook. These limits have no public tuning knob. |
| Shutdown/cancellation does not finish | First quiesce data operations. Check an uncooperative client or non-terminating serializer/model hook; Python cannot kill its running thread. |
| Growing TTL lag | Correlate active parts, merges, disk, clock synchronization, and server TTL settings. Logical expiry can still be correct while physical rows remain. |
| Managed table in async queue | Identify the other writer or settings path. Package writes are synchronous and do not expose an override. |
| Literal IPv6 configuration error | Use a DNS hostname that resolves to IPv6 or an IPv4/DNS endpoint; literal IPv6 is outside v0.1. |

Public errors are code-only and detach raw driver causes. Investigate sensitive
details in access-controlled ClickHouse/server logs rather than adding
credentials, payloads, task logs, or raw exception objects to application logs.

## Security checklist

- Permit result/progress `INSERT` only to trusted producers. JSON is not an
  untrusted-row sandbox because Taskiq reconstructs exception classes.
- Treat every Pickle-capable table writer as able to execute code in readers.
- Keep TLS verification enabled; use `ca_cert` for private trust roots rather
  than disabling verification.
- Put credentials in the trusted factory's secret provider, not CLI arguments,
  source control, exception messages, or monitoring labels.
- Separate migrator, data-worker, and monitoring privileges where practical.
- Protect permanent metadata and migration history from manual writes.
- Do not attach constraints, indexes, projections, or materialized views to
  managed tables in v0.1.
- Keep ClickHouse clocks synchronized and preserve backups for managed data and
  permanent metadata together.
