# User guide

This guide describes the public v0.1 behavior implemented by
`ClickHouseResultBackend`. The [operator guide](operator-guide.md) covers schema
pre-provisioning, grants, rollout, recovery, and monitoring.

## Public surface

Use the package root for the backend, `SchemaMode`, and public exceptions:

```python
from taskiq_clickhouse import (
    ClickHouseResultBackend,
    ClickHouseResultNotFoundError,
    ResultPersistenceReceiver,
    SchemaMode,
)
```

Taskiq models and serializers are not re-exported. Import them from Taskiq.
`ClickHouseSchemaManager` is an operator surface and is imported from
`taskiq_clickhouse.schema`, not the package root.

The backend implements Taskiq's async result-backend methods:

```python
await backend.startup()
await backend.shutdown()
await backend.set_result(task_id, result)
ready = await backend.is_result_ready(task_id)
result = await backend.get_result(task_id, with_logs=False)
await backend.set_progress(task_id, progress)
progress = await backend.get_progress(task_id)
```

`task_id` must be an exact `str`; `with_logs` must be an exact `bool`.

## Constructor reference

The constructor is keyword-only. It validates configuration without creating a
client, performing network I/O, scheduling a task, or starting a thread.

| Option | Type | Default | Contract |
| --- | --- | --- | --- |
| `host` | `str` | required | IPv4 literal or DNS host only; no URL scheme, credentials, path, query, explicit port, whitespace, brackets, or literal IPv6 |
| `database` | `str` | required | Existing database; one identifier matching `[A-Za-z_][A-Za-z0-9_]{0,126}` |
| `secure` | `bool` | required | Exact boolean; selects HTTPS when true and HTTP when false |
| `result_ttl` | `timedelta` | required | Finite positive logical-visibility duration |
| `purge_ttl` | `timedelta` | required | Finite physical-retention duration strictly greater than `result_ttl` |
| `port` | `int \| None` | `None` | Exact non-boolean integer in `1..65535`; `None` leaves interface-default selection to `clickhouse-connect` |
| `username` | `str \| None` | `None` | Basic/default-user or mTLS identity; `""` normalizes to `None` |
| `password` | `str` | `""` | Exact string; a password without a username authenticates the ClickHouse default user |
| `access_token` | `str \| None` | `None` | Non-empty bearer token; TLS-only and mutually exclusive with username/password/client certificate |
| `ca_cert` | `str \| None` | `None` | TLS-only private CA path passed to the driver; verification remains enabled |
| `client_cert` | `str \| None` | `None` | TLS-only client certificate path; selects mTLS mode |
| `client_cert_key` | `str \| None` | `None` | Optional client key path; requires `client_cert` |
| `server_host_name` | `str \| None` | `None` | TLS-only server-name override used for certificate verification |
| `connect_timeout` | `int` | `10` | Positive, non-boolean driver connect timeout in seconds |
| `send_receive_timeout` | `int` | `300` | Positive, non-boolean driver send/receive timeout in seconds |
| `namespace` | `str` | `"default"` | Storage key matching `[A-Za-z0-9][A-Za-z0-9._:-]{0,127}` |
| `result_table` | `str` | `"taskiq_clickhouse_results"` | Single identifier in `database` |
| `progress_table` | `str` | `"taskiq_clickhouse_progress"` | Single identifier in `database` |
| `keep_results` | `bool` | `True` | Exact boolean; false enables best-effort consume after a successful decode |
| `serializer` | `TaskiqSerializer \| None` | `None` | `None` creates the package's exact default JSON serializer |
| `serializer_id` | `str \| None` | `None` | Stable storage id; inferred only for exact supported built-ins |
| `schema_mode` | `Literal["migrate", "validate"]` | `"migrate"` | Worker startup migration policy |

Textual constructor values must be exact built-in `str` objects; the backend
does not coerce path-like objects, numbers, or string subclasses. Database and
table options all use the identifier grammar
`[A-Za-z_][A-Za-z0-9_]{0,126}`. Optional TLS paths and the server-name override
must be non-empty when supplied. Construction validates their shape but does
not read certificate files, resolve DNS, authenticate, or check database/table
existence; those effects begin at startup.

The result table, progress table, and fixed
`taskiq_clickhouse_metadata` table must have three different names. The
configured database must already exist; v0.1 never creates it.

### Endpoint and authentication modes

DNS names that resolve to IPv6 are supported, but literal IPv6 host strings are
rejected. This is a v0.1 driver/proxy boundary, not a claim that ClickHouse
lacks IPv6 support.

TLS verification is always enabled (`verify=True`); there is no public
`verify=False`. `ca_cert`, client-certificate fields, and `server_host_name`
require `secure=True`.

Choose exactly one authentication shape:

| Mode | Required | Forbidden |
| --- | --- | --- |
| Basic/default user | Optional username and password | Bearer token and client certificate |
| Bearer token | `secure=True`, non-empty `access_token` | Username, non-empty password, client certificate |
| Mutual TLS | `secure=True`, `client_cert`, explicit non-empty username, empty password | Bearer token |

The optional client key requires the client certificate. A private CA or
server-name override can accompany either TLS authentication mode.

The package also owns `verify=True`, aware timezone mode, disabled automatic
session ids, two driver query retries, and its client name. Callers cannot
provide a DSN, pre-created client, arbitrary headers/settings, retry counts,
session ids, or acknowledgement overrides.

### Retention input

Both TTL values must be exact `datetime.timedelta` instances. They are
normalized to integer microseconds; floats and duration strings are not
accepted. The required order is:

```text
0 < result_ttl < purge_ttl
```

There is no infinite-retention mode. Durations and every computed deadline must
fit the supported `DateTime64(6, 'UTC')` interval from
`1900-01-01 00:00:00.000000 UTC` through
`2299-12-31 23:59:59.999999 UTC`.

## Taskiq and direct lifecycle

Taskiq's base `AsyncBroker` lifecycle calls an attached backend after broker
startup and after broker shutdown handlers and middleware. Concrete brokers may
override that lifecycle. In the supported Taskiq release, `InMemoryBroker`
does so without calling the result backend. Its owner must therefore start the
backend explicitly and, after stopping new submissions, clean up in this order:

```python
try:
    await broker.startup()
    await backend.startup()
    # Submit and await operations here.
    ...
finally:
    try:
        await broker.wait_all()
    finally:
        try:
            await broker.shutdown()
        finally:
            await backend.shutdown()
```

For a broker that retains Taskiq's base lifecycle, let that broker own the
backend and do not add a second manual lifecycle around the same instance.
Check a third-party broker's lifecycle contract instead of inferring ownership
from `with_result_backend()` alone.

Direct callers must explicitly start and stop it:

```python
await backend.startup()
try:
    # Submit and await backend operations here.
    ...
finally:
    # Stop new operations and await every in-flight operation first.
    await backend.shutdown()
```

The lifecycle is:

| State | Allowed behavior |
| --- | --- |
| `NEW` | `startup()` or schema-manager lease; data/progress calls fail before I/O |
| `STARTING` | Same-loop concurrent `startup()` waits for that transition; data/progress calls fail |
| `READY` | Data/progress calls are allowed in the owning PID and event loop; repeated `startup()` is a no-op there |
| `CLOSED` | Repeated `shutdown()` is a no-op; startup and data/progress calls fail |

`shutdown()` on a `NEW` backend closes it without creating a client. A failed or
cancelled startup closes any acquired client and returns the instance to `NEW`.
A successfully closed instance cannot be restarted.

First startup binds the backend to the current PID and running event loop.
Using a started instance after `fork()`, or from a different loop in the same
process, fails before touching the inherited client. A still-`NEW` instance can
be constructed before a fork and started in the child.

### Result-gated receiver

`ClickHouseResultBackend` deliberately does not own broker deliveries. Taskiq
0.12.4's stock receiver catches a failing `set_result()` call and then reaches
its `when_saved` ACK branch. Use `ResultPersistenceReceiver` when the broker
message must remain unacknowledged unless the complete result-save phase
succeeds.

The v0.1 qualification is the Taskiq CLI worker path only:

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

The receiver accepts only `ack_type=None` or `when_saved`, normalizing both to
`when_saved`, and rejects any configuration other than `max_async_tasks=1`,
`max_async_tasks_jitter=0`, and `max_prefetch=1`. Its
`wait_tasks_timeout` is mandatory, finite, and positive. The timeout bounds
Taskiq's graceful callback wait and an awaitable broker ACK. It does not bound
the task function, a synchronous thread-pool call, dependency/middleware code
before the failure, or process termination, and it does not replace backend
connection and send/receive timeouts. The deployment supervisor must impose a
hard termination budget.
Each `ResultPersistenceReceiver` instance is single-use because Taskiq's runner
consumes internal semaphore state. The CLI creates one receiver per child; a
second `listen()` call on the same instance fails immediately.

For an ackable delivery, the receiver gives Taskiq a deferred ACK gate. Taskiq
can request acknowledgement during its callback, but the original broker ACK
is invoked exactly once only after the callback, including result persistence
and `post_save` middleware, returns successfully. A persistence error,
cancellation before original ACK invocation, or malformed ACK sequence leaves
the original delivery unacknowledged and stops the consumer. An exception,
cancellation, or timeout during the original ACK is an ambiguous settlement
outcome: the receiver never retries that ACK in-process and stops the consumer
so the broker remains the source of truth. After the first local failure it
admits no further prefetched callback in that receiver child. Taskiq remains
the broker lifecycle owner: its CLI shutdown closes the channel, and RabbitMQ decides whether the
delivery is redelivered or dead-lettered.

`taskiq-aio-pika` is not a core dependency; the qualified path installs version
0.6.0 separately. The qualified broker topology is
`AioPikaBroker(qos=1)` with a quorum task queue, an explicit positive delivery
limit, and a dead-letter queue. The
qualified recovery sequence is controlled: one consumer exits, all competing
consumers remain quiesced, ClickHouse recovers, and one replacement consumer is
started. `--max-fails 1` prevents immediate replacement in the demonstrated
single-consumer process. A live sibling process or replica can otherwise take
the requeued delivery immediately and consume the finite delivery budget while
ClickHouse remains unavailable. In that wider topology, bounded DLQ routing is
the guarantee; automatic transient recovery is not.

The stock receiver, non-ackable messages, `qos` values other than one, classic
queues without a delivery limit, direct/manual receiver lifecycle, and earlier
Taskiq ACK policies are not equivalent paths. Fleet quiescing, restart
backoff/health checks, DLQ replay, and process hard-kill policy are application
and supervisor responsibilities.

This is at-least-once execution, not exactly-once. Application effects may have
committed before final-result persistence failed. Tasks must therefore be
idempotent or use an application-owned deduplication key; the backend's result
generation identity is not a transaction or deduplication token for task-side
effects.

### Quiescence and cancellation

Backend shutdown owns client cleanup, but it is not a data-operation drain.
Direct users must stop admission and await all active `set_*`, `get_*`, and
readiness calls before shutdown. Closing the shared client concurrently with
those calls has no completion guarantee.

Client creation and close each run as one package-owned shielded task. If the
caller is cancelled, the backend continues waiting for that exact task to reach
a terminal state, performs cleanup exactly once, and then restores the first
outer cancellation. A second raw client close is not used as recovery. There is
no package-wide arbitrary 30-second cleanup deadline; configured driver
connection/send-receive timeouts bound normal production I/O, while an
uncooperative dependency can still delay termination.

## Serialization and stored format

The distribution requires `pydantic>=2.7,<3`; 2.7 is the first supported
release with the strict serialization-warning contract used here. The package
owns a Pydantic 2 Python-mode boundary and calls
`model_dump(mode="python", warnings="error")`. For final results it takes one
exact model snapshot, removes `log`, and passes the remaining mapping to the
configured Taskiq serializer. It serializes `log` separately with the same
serializer. Progress is dumped as its exact `state`/`meta` Python mapping.
Decode requires the exact expected key sets and strict Taskiq model validation;
there is no fallback to another serializer.

The stored result mapping has exactly `is_err`, `return_value`,
`execution_time`, `labels`, and `error`; the log is the separate value. A
decoded progress mapping has exactly `state` and `meta`. Missing or unexpected
keys, coercible-but-wrong scalar types, and a non-`str | None` decoded log fail
closed as corruption rather than taking Pydantic defaults.

Callers must not mutate a `TaskiqResult`, `TaskProgress`, or values reachable
from either model while the corresponding `set_*` call is running. The package
takes an internally coherent top-level snapshot; it does not provide ownership,
locking, or copy-on-write semantics for caller-mutated object graphs.

The payload-format id is `taskiq-pydantic2-python-v1`. It is persisted with the
serializer id, table scope, namespace, and TTLs in permanent metadata. Rows do
not carry a serializer id. Consequently, changing the serializer, serializer
id, TTLs, payload format, or result/progress table pair for an existing scoped
namespace fails the startup barrier even if old data looks expired.

### Default JSON

`serializer=None` creates exactly:

```python
JSONSerializer(default=None, ensure_ascii=True)
```

Its stable id is `taskiq-json-v1`. JSON accepts only graphs supported by that
serializer after Python-mode model extraction. Values such as raw bytes, sets,
datetimes, and arbitrary application objects fail with a safe encode error;
the backend does not coerce them or switch codecs.

Standard JSON normalization still applies to accepted values: tuples decode as
lists, supported non-string mapping keys such as integers decode as strings,
and `ensure_ascii=True` escapes non-ASCII characters only in stored bytes—the
decoded Unicode value is unchanged. Use a versioned custom serializer or
trusted Pickle only when those Python type distinctions are part of the result
contract.

### Pickle

Pickle is an explicit opt-in and receives `taskiq-pickle-v1`:

```python
from taskiq.serializers.pickle import PickleSerializer

backend = ClickHouseResultBackend[object](
    # endpoint, storage, and TTL options omitted here
    serializer=PickleSerializer(),
)
```

Pickle preserves a broader Python graph, but unpickling attacker-controlled
bytes can execute arbitrary code. Use it only when every process and principal
able to write the managed tables is trusted. Taskiq still normalizes its
exception field during model dumping, so Pickle does not promise preservation
of a live exception object's identity.

### Custom serializers

A custom or non-default configured `TaskiqSerializer` requires an explicit,
versioned id:

```python
backend = ClickHouseResultBackend[object](
    # endpoint, storage, and TTL options omitted here
    serializer=application_serializer,
    serializer_id="application-json-v2",
)
```

The id must match `[A-Za-z][A-Za-z0-9._-]{0,63}`. The built-in ids are
reserved. An exact default `JSONSerializer` or exact `PickleSerializer` may
omit the id; if supplied, its id must equal the corresponding built-in id.
Configured JSON variants, subclasses, and all other serializers require a
custom id. Module/class names are not used as durable ids.

An exact canonical `JSONSerializer` or exact `PickleSerializer` supplied by a
caller is validated and replaced with a fresh package-owned built-in instance.
Mutating the caller's original object after backend construction therefore
cannot change wire behavior under a reserved id. A custom serializer is kept
by reference because its application-specific configuration cannot be cloned
generically; its implementation and mutable configuration must remain stable
for the complete backend lifetime.

`dumpb` must return exact `bytes`. One backend uses one capacity-one FIFO gate
for all calls to its configured serializer, including result payloads, logs,
and progress, so those calls do not overlap within that backend. Gates are not
global: another backend or application code using the same serializer object
can overlap these calls. Such a shared serializer must therefore be
thread-safe. Even sequential calls can run on different pool threads, so a
custom serializer must not be thread-affine. Serializer and model hooks must
terminate: Python cannot interrupt a running thread.

Synchronous boundary jobs use lazy, process-owned, PID-aware thread pools, not
the asyncio default executor. Result model snapshots use a separate one-worker,
one-submission lane to protect Taskiq's process-global exception recursion
cache. Other model and serializer jobs share a process-wide pool with the
platform `ThreadPoolExecutor` worker default and a fixed limit of 32 submitted
jobs, counting both running and executor-queued work. After a fork, a child
discards inherited pool and admission state and creates its own state lazily.

Both the per-backend serializer gate and process-pool admission hand slots to
live queued callers in FIFO order. A caller beyond a limit awaits a slot
asynchronously in its own task rather than entering the executor queue; this is
the package's bounded backpressure. It does not guarantee completion order once
multiple jobs are running. Each submitted hook receives a fresh copy of the
caller's `contextvars` context; `ContextVar` bindings changed by one hook do
not leak into another job (objects stored in those bindings are not deep-copied).

Cancellation while waiting for either admission gate removes that waiter and
does not submit its synchronous operation. Once submitted, outer cancellation
does not abandon or retry the job. The caller waits for that exact job to
terminate and then receives its first `CancelledError`; a terminal
process-level `BaseException` from the job takes priority. A non-terminating
custom hook can therefore hold a slot, submitted-call cancellation, and
interpreter exit indefinitely.

The limits above are fixed implementation policy, not public configuration.
`backend.shutdown()` does not shut down these process-wide pools. Bound Taskiq
concurrency at the application level too, and benchmark the workload; the
package exposes no pool-size or admission-limit knob.

## Result, log, and progress behavior

### Latest result and readiness

Every result/progress write that reaches allocation creates a new append-only
logical generation. The allocator uses ClickHouse server time, the stored
maximum generation, and a UUIDv4 tie-breaker. A caller retry after an error is
a fresh invocation with no idempotency token; if it reaches allocation, it
receives a fresh generation.

Reads first select the latest total-order row across all purge partitions, then
interpret tombstone state and the exclusive visibility deadline. Filtering
expired rows first would resurrect older history and is deliberately forbidden.
`is_result_ready()` is metadata-only and does not select payload columns.

Missing, expired, and consumed final results all produce
`ClickHouseResultNotFoundError` from direct `get_result()`. Readiness returns
`False` for those states. Authentication, timeout, schema, malformed-row, and
decode failures remain errors; they are never reduced to `False` or “missing.”
Taskiq's `AsyncTaskiqTask` wrappers may wrap backend errors in Taskiq result
access errors.

### Write acknowledgement

Production queries and migration statements are packaged as dedicated `.sql`
files and loaded once with `pathlib`, independently of the process working
directory. Values and validated database/table identifiers are supplied through
typed `clickhouse-connect` parameters; the package does not interpolate them
with Python string formatting. Structural SQL such as projections, column
definitions, engines, keys, and TTL clauses remains fixed in those files.
Result, progress, tombstone, and metadata payloads still use the driver's native
insert API rather than SQL value templates. Integration fixtures retain
independent golden SQL only where it is the contract oracle, and operator
examples remain documentation rather than executable application code.

Result, progress, tombstone, and metadata writes use package-owned synchronous
settings:

```python
{"async_insert": 0, "wait_for_async_insert": 1, "wait_end_of_query": 1}
```

Correctness reads disable the query cache. On an ambiguous transport/stream
response, the package confirms the exact frozen row; confirmed absence permits
one retry of that same row, followed by one final confirmation. A confirmed
row is success, a final confirmed absence is an I/O error, and an ambiguous
confirmation remains an explicitly ambiguous I/O error. Definite database,
schema, authentication, and local encode failures are not retried.

Driver retries or response loss can leave duplicate physical copies of one
identical logical row. New public method invocations are not deduplicated.
Caller cancellation propagates without a package retry and is not a
transactional rollback signal.

### Logs

`result_payload` never contains `TaskiqResult.log`; `log_payload` stores the
separately serialized `str | None` value.

- `get_result(task_id, with_logs=False)` selects and decodes only
  `result_payload`, and returns `log=None`.
- `get_result(task_id, with_logs=True)` selects result and log from the same
  latest row and restores the log before strict validation.

The no-log path proves a narrower SQL/network projection. It does not promise
that ClickHouse reads fewer physical bytes for every part layout. Taskiq
currently marks `TaskiqResult.log` as deprecated; this package supports the
field only across its checked pre-1.0 Taskiq model contract and fails
construction if that consumed model shape changes.

### `keep_results=False`

Consume mode is globally visible best effort, not atomic `GETDEL` and not an
exactly-once delivery primitive:

1. Select the latest visible result and requested log projection.
2. Fully decode and validate it.
3. Append an acknowledged tombstone for that exact generation.
4. Return the decoded result only after tombstone acknowledgement succeeds.

Two readers can both finish step 2 and both return the same result. Decode
failure does not consume. Lost tombstone acknowledgement can leave an
ambiguous error/outcome, and the result is not returned on an unconfirmed
tombstone.

The tombstone reuses the selected generation and wins only inside that
generation. It is bound to the selected namespace, task id, and qualified
result table; there is no separate task-id input that can redirect it. It keeps
the selected `visible_until`, uses the read's ClickHouse `observed_at` as its
`written_at`, and sets
`purge_at = max(selected.purge_at, observed_at + purge_ttl)`.

In `read A -> write newer B -> tombstone A`, B remains latest and visible. A
later explicit `set_result()` creates a new generation and can make a task id
ready again.

### Progress

Progress uses a separate `MergeTree` table and latest-generation query. It
shares the namespace, serializer, `result_ttl`, and `purge_ttl` contract.
Missing or expired progress returns `None`; progress is never consumed.

Progress writes neither create nor delete a final result. Final-result writes
do not create, delete, or determine progress. A final result can be ready while
progress is absent or expired, and vice versa.

## Retention and time

Each new result or progress row stores:

- `written_at`: ClickHouse observation time;
- `visible_until = written_at + result_ttl`;
- `purge_at = max(written_at + purge_ttl, latest stored purge_at)`.

`visible_until` is exclusive: equality with the read's ClickHouse
`observed_at` means unavailable. The process clock is not used for storage
ordering or visibility.

`purge_at` is the earliest time at which ClickHouse's table TTL may physically
delete a row. Deletion is asynchronous and depends on background merges; it is
not immediate or scheduled to an exact second. The intentional interval
between `visible_until` and `purge_at` retains invisible history and
tombstones beyond logical visibility.

New writes retain the stored maximum `purge_at`, and tombstones retain at least
the selected row's deadline. These floors order each suppressing row's earliest
TTL eligibility after the retained deadline observed during allocation. They do
not make cleanup atomic across MergeTree parts: ClickHouse can delete a newer
suppressing row while an older part is still waiting for a TTL merge. If the
endpoint clock subsequently moves backward across the older row's visibility
window, that retained row can become visible again.

v0.1 therefore requires one logical endpoint with synchronized server clocks
and does not guarantee non-resurrection after partial physical TTL cleanup plus
a later clock rollback. It has no durable, non-TTL per-task suppression
watermark. Treat a backward jump across already-cleaned visibility windows as
outside the v0.1 consistency contract.

## Topology boundary

All ordering, observation time, schema inspection, and acknowledgements assume
one logical endpoint with one coherent catalog and read-after-DDL/read-after-
write visibility. v0.1 does not emit `ON CLUSTER`, create `Distributed` or
replicated tables, manage sharding/replication, or promise cross-replica
visibility. Do not put a load balancer that can expose divergent catalogs or
replica visibility behind the configured endpoint and infer support from a
successful connection.

## Errors and secrets

All public package exceptions derive from Taskiq's `ResultBackendError`:

```text
ClickHouseResultBackendError
├── ClickHouseConfigurationError
├── ClickHouseLifecycleError
├── ClickHouseSchemaError
│   ├── ClickHouseMigrationError
│   ├── ClickHouseSchemaDriftError
│   └── ClickHouseNamespaceError
├── ClickHouseSerializationError
│   ├── ClickHouseEncodeError
│   └── ClickHouseDecodeError
├── ClickHouseDataCorruptionError
├── ClickHouseResultNotFoundError
├── ClickHouseProgressError
└── ClickHouseBackendIOError
```

Public messages contain package-owned `operation:reason` codes. Driver
messages and traceback causes are detached so credentials, endpoint details,
payloads, and task logs are not exposed through package errors. Do not log
constructor secrets or serializer inputs yourself.

Even with JSON, managed result tables are trusted-writer boundaries: Taskiq may
reconstruct and instantiate application exception classes named by stored
metadata. Pickle additionally permits arbitrary-code deserialization. Restrict
table `INSERT`, schema DDL, and metadata writes to trusted principals; see the
[privilege matrix](operator-guide.md#privilege-matrix).
