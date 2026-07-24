# taskiq-clickhouse

`taskiq-clickhouse` is an async ClickHouse result backend for Taskiq. It stores
final results, optional result logs, and task progress in append-only
`MergeTree` tables, with a startup schema barrier and finite retention.

The v0.1 contract is deliberately narrow: one logical ClickHouse endpoint, one
database that already exists, and no `ON CLUSTER`, replication, sharding, or
cross-replica consistency guarantee.

## Install

Install the distribution from the package index or artifact channel used by
your deployment:

```console
python -m pip install taskiq-clickhouse
```

From a source checkout, use:

```console
python -m pip install .
```

The core package does not install a Taskiq broker transport. For the exact
RabbitMQ path qualified below, install `taskiq-aio-pika==0.6.0` separately in
the application environment.

## Compatibility

| Component | Declared boundary | Qualification profile |
| --- | --- | --- |
| Python | `>=3.11` | 3.11, 3.12, 3.13, and 3.14 |
| Taskiq | `>=0.12.4,<1` | 0.12.4 and the newest resolvable pre-1.0 release |
| Pydantic | `>=2.7,<3` | 2.7.0 and the newest resolvable pre-3 release |
| `clickhouse-connect` | `>=1.4.2,<2` | 1.4.2 and 1.5.0 |
| ClickHouse server | minimum 25.8.28.1 | 25.8.28.1 and 26.6.1.1193 |
| `taskiq-aio-pika` | optional broker integration, installed separately | 0.6.0 for the qualified RabbitMQ path |
| RabbitMQ server | external broker | 4.3.2 quorum queue for the qualified RabbitMQ path |

Python versions newer than 3.14 are not artificially excluded by package
metadata, but they are outside the current qualification matrix and transitive
dependencies may impose their own interpreter bounds. Profiles in this table
are release targets, not proof that every cell passed against an arbitrary
checkout; use the recorded local release/matrix artifacts for evidence. The
[GitHub CI workflow](https://github.com/AmberFog/taskiq_clickhouse/blob/main/.github/workflows/ci.yml)
runs native Python/Taskiq/Pydantic and ClickHouse/client matrices, the real
ClickHouse integration suite, and a clean package-install smoke on branch
pushes, pull requests and explicit manual runs.
The [storage POC report](https://github.com/AmberFog/taskiq_clickhouse/blob/main/docs/storage-poc-report.md)
records the pinned run that
established the single-endpoint ClickHouse/client boundary.
See the [0.1.0rc1 release notes](https://github.com/AmberFog/taskiq_clickhouse/blob/main/docs/release-notes-0.1.0rc1.md)
for the candidate contract and known limitations.

## Quick start

Attach the backend to a Taskiq broker. The example uses Taskiq's in-memory
broker only to keep the integration visible; use the broker appropriate for
your application.

```python
from datetime import timedelta
import os

from taskiq import InMemoryBroker
from taskiq_clickhouse import ClickHouseResultBackend


result_backend = ClickHouseResultBackend[object](
    host="clickhouse.example.com",
    port=8443,
    username="taskiq_worker",
    password=os.environ["CLICKHOUSE_PASSWORD"],
    database="application",
    secure=True,
    result_ttl=timedelta(days=1),
    purge_ttl=timedelta(days=7),
    namespace="production",
    schema_mode="validate",
)

broker = InMemoryBroker(await_inplace=True).with_result_backend(result_backend)


@broker.task
async def add(left: int, right: int) -> int:
    return left + right


async def run_once() -> int:
    try:
        await broker.startup()
        await result_backend.startup()
        task = await add.kiq(2, 3)
        result = await task.wait_result(check_interval=0, timeout=10)
        return result.return_value
    finally:
        # Stop new submissions before this cleanup starts.
        try:
            await broker.wait_all()
        finally:
            try:
                await broker.shutdown()
            finally:
                await result_backend.shutdown()
```

Taskiq's base broker lifecycle calls the attached result backend's `startup()`
and `shutdown()`. `InMemoryBroker` is an exception in the supported Taskiq
release: it overrides that lifecycle without calling the backend, so the
one-shot example owns the backend explicitly. Do not duplicate this explicit
ownership for a broker that already owns its result backend. Before starting
workers with `schema_mode="validate"`, run a controlled migration using a
trusted synchronous factory that returns a fresh backend:

```console
taskiq-clickhouse-schema migrate your_package.taskiq_app:make_result_backend
taskiq-clickhouse-schema validate your_package.taskiq_app:make_result_backend
```

A complete syntax-checkable one-shot in-memory flow and factory is in
[`docs/examples/taskiq_app.py`](https://github.com/AmberFog/taskiq_clickhouse/blob/main/docs/examples/taskiq_app.py).

For direct use, own the lifecycle of an instance that is not also owned by a
broker, and let all data operations finish before shutdown:

```python
async def probe_result(backend: ClickHouseResultBackend[object]) -> bool:
    await backend.startup()
    try:
        return await backend.is_result_ready("task-id")
    finally:
        await backend.shutdown()
```

Construction does not perform network I/O or start threads. A backend becomes
bound to the process and event loop that starts it, and a closed instance cannot
be restarted.

### Result-gated RabbitMQ acknowledgement

Taskiq 0.12.4's stock receiver catches a final-result persistence error and
still acknowledges an AioPikaBroker delivery under `when_saved`. Deployments
that require result persistence to gate RabbitMQ acknowledgement must select
the package receiver explicitly:

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

The receiver requires `when_saved` acknowledgement, exactly one in-flight
callback, zero concurrency jitter, one Taskiq prefetch slot, and a finite
positive `wait_tasks_timeout`; unsafe values fail during worker construction.
Configure `AioPikaBroker(qos=1)` as well. The qualified failure-recovery
topology is one active consumer with a RabbitMQ quorum task queue, a positive
delivery limit, and a dead-letter queue.
Each receiver instance is single-use; Taskiq's CLI constructs one per worker
child. Do not call `listen()` twice on the same instance.
The receiver defers the broker ACK until Taskiq's complete result-save phase
returns. A save-phase failure withholds ACK. An exception, cancellation, or
timeout after the ACK call begins has an ambiguous broker outcome. In either
case the receiver closes local admission and exits the consumer child without
an in-process ACK retry.

This is a controlled replay contract, not automatic transient recovery.
`--max-fails 1` prevents the qualified Taskiq parent from immediately replacing
the failed child. Restore ClickHouse first, then start one replacement consumer;
if the delivery has already reached the DLQ, inspect and replay it explicitly.
Other worker processes or replicas can consume the requeued message immediately
and exhaust the finite delivery budget while ClickHouse is still unavailable.
RabbitMQ then provides the guaranteed bounded outcome: DLQ, not eventual
success. Fleet-wide quiescing, health-gated restart/backoff, DLQ monitoring, and
a hard process-termination budget belong to the deployment supervisor.

Redelivery means at-least-once task execution. The task function may have
committed effects before result persistence failed, so every task on this path
must be idempotent or own an application-level deduplication key.
`wait_tasks_timeout` also bounds an awaitable broker ACK, but it does not
time-limit application code or replace backend I/O timeouts and supervisor
hard-kill policy. The stock receiver, non-ackable brokers, `qos` values other
than one, classic queues without a bounded delivery policy, earlier ACK modes,
and manually embedded receiver lifecycles are outside this qualification. See the
[operator guide](https://github.com/AmberFog/taskiq_clickhouse/blob/main/docs/operator-guide.md)
for the exact RabbitMQ topology and recovery contract.

## Important semantics

- Retention is mandatory: `0 < result_ttl < purge_ttl`. `visible_until` is an
  exclusive API deadline; ClickHouse TTL deletion after `purge_at` is eventual.
  v0.1 requires a synchronized endpoint clock and does not prevent an older row
  from resurfacing after partial TTL cleanup followed by a later clock rollback.
- `keep_results=False` is best-effort consume, not atomic `GETDEL`. Concurrent
  readers can both receive the same result. A targeted tombstone for result A
  cannot hide a newer result B.
- Results and logs are stored separately. `get_result(..., with_logs=False)`
  does not select or deserialize `log_payload`; this is a projection/network
  guarantee, not a claim about physical disk reads.
- Progress has its own table. It shares namespace, serializer, and retention,
  but it is never consumed and does not affect final-result readiness.
- JSON is the default serializer. JSON still requires trusted table writers
  because Taskiq reconstructs stored exception types. Pickle is opt-in and can
  execute arbitrary code while deserializing malicious rows.
- Package writes are synchronous and use package-owned acknowledgement
  settings. There is no public settings override or fire-and-forget mode.
- Broker-message acknowledgement is a separate receiver boundary. Use the
  explicit qualified `ResultPersistenceReceiver` CLI path when final-result
  persistence must gate RabbitMQ ACK; the backend never owns broker delivery.
- Production SQL is shipped as package data and loaded once from dedicated
  `.sql` files. Runtime values and validated database/table identifiers use
  typed ClickHouse parameters; SQL structure remains fixed, and payload writes
  continue to use the driver's native insert API.
- Synchronous model/serializer boundaries use fixed, bounded admission before
  executor submission. There is no built-in metrics endpoint or public
  pool/admission tuning API in v0.1.

Read the [user guide](https://github.com/AmberFog/taskiq_clickhouse/blob/main/docs/user-guide.md)
for every constructor option,
lifecycle, serialization, result, log, progress, and retention contract. Read
the [operator guide](https://github.com/AmberFog/taskiq_clickhouse/blob/main/docs/operator-guide.md)
for grants, schema rollout,
recovery, physical drift checks, operational queries, and troubleshooting.

## Capacity qualification

The package does not promise a universal latency or throughput SLO. Validate
your retained-history depth, payload sizes, concurrency, TTL lag, hardware, and
ClickHouse settings with a representative deployment-owned workload before
rollout. Performance diagnostics are intentionally outside package CI.

## Development checks

The repository's default test command excludes real-ClickHouse tests:

```console
uv run pytest tests/test_documentation.py
uv run pytest
```

The focused documentation check compiles every maintained Python example,
verifies local Markdown files and heading fragments, and pins the explicit
`InMemoryBroker` result-backend lifecycle.

The concise local source, service and package procedure is in the
[local release checklist](https://github.com/AmberFog/taskiq_clickhouse/blob/main/docs/release-checklist.md).
GitHub CI expresses compatibility directly as native matrices, builds the
wheel and sdist once, clean-installs the wheel outside the checkout, and exposes
one aggregate `Required gate` check. It does not tag or publish a release.
