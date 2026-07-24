# 0.1.0rc1 release notes

`0.1.0rc1` is the first local release candidate of `taskiq-clickhouse`. It is
intended for qualification before the separately authorized tag and package
publication step.

## Compatibility contract

| Component | Declared range | Local qualification boundary |
| --- | --- | --- |
| Python | `>=3.11` | CPython 3.11, 3.12, 3.13, and 3.14 |
| Taskiq | `>=0.12.4,<1` | 0.12.4 and newest resolvable pre-1.0 |
| Pydantic | `>=2.7,<3` | 2.7.0 and newest resolvable pre-3 |
| `clickhouse-connect` | `>=1.4.2,<2` | 1.4.2 and newest resolvable pre-2 |
| ClickHouse | minimum 25.8.28.1 | 25.8.28.1 and 26.6.1.1193 |
| `taskiq-aio-pika` | optional, installed separately | 0.6.0 for the qualified RabbitMQ path |
| RabbitMQ | external broker | 4.3.2 quorum queue for the qualified RabbitMQ path |

The Python metadata deliberately has no artificial upper bound. Versions newer
than 3.14 are not qualified by this candidate and may be constrained by
transitive dependencies.

## Install or upgrade

Install the wheel selected by the release owner:

```console
python -m pip install taskiq_clickhouse-0.1.0rc1-py3-none-any.whl
```

This is the first release candidate, so there is no earlier persisted package
format to upgrade. Before starting validate-only workers, create or reconcile
the schema with the controlled migration principal:

```console
taskiq-clickhouse-schema migrate your_package.taskiq_app:make_result_backend
taskiq-clickhouse-schema validate your_package.taskiq_app:make_result_backend
```

Namespace registration freezes the qualified tables, serializer id, payload
format, and both retention values. Do not change those settings in place; use a
new namespace or a future explicit data migration.

## Behavior to account for

- `result_ttl` and `purge_ttl` are mandatory and satisfy
  `0 < result_ttl < purge_ttl`.
- `visible_until` controls exclusive logical visibility. `purge_at` is later
  and ClickHouse removes eligible rows eventually, not at an exact instant.
- Purge-deadline floors assume a synchronized endpoint clock. They are not a
  durable watermark: partial TTL cleanup followed by a later clock rollback can
  expose retained older history and is outside the v0.1 consistency contract.
- `keep_results=False` performs an acknowledged, generation-targeted
  tombstone write after the read. It prevents consuming generation A from
  hiding newer generation B, but it does not prevent two readers from both
  receiving A.
- Result payload and log payload are separate. Omitting logs avoids returning
  and decoding the large log column; it does not claim a physical disk-I/O
  reduction.
- Progress uses a separate table, is never consumed, and does not make a final
  result ready.
- JSON is the default. The table must still have trusted writers because Taskiq
  can reconstruct application exception types. Pickle can execute arbitrary
  code during deserialization and must only read trusted rows.
- Exact built-in JSON and Pickle inputs are copied into package-owned canonical
  instances so caller mutation cannot alter a reserved wire identity. Custom
  serializers remain caller-owned references and must stay stable and
  thread-safe for the backend lifetime.
- The backend owns one process-local client and one event loop after startup.
  A closed backend is terminal and cannot be restarted.
- A real AioPikaBroker worker outage exposed a Taskiq 0.12.4 stock-receiver
  limitation: `when_saved` logs final-result persistence failure but still
  acknowledges the RabbitMQ message. The backend reports the write failure
  correctly; the surrounding receiver owns the later ACK. The candidate adds
  an explicit `ResultPersistenceReceiver` CLI path that defers the actual ACK
  until the complete save phase succeeds and stops the consumer after failure.
  Qualification additionally requires `when_saved`, one in-flight callback,
  zero jitter, one Taskiq prefetch slot, a finite graceful callback-wait and
  awaitable-ACK timeout,
  `AioPikaBroker(qos=1)`, and a RabbitMQ quorum queue with a positive delivery
  limit and DLQ. The demonstrated recovery disables automatic child restart,
  restores ClickHouse, and then starts one replacement consumer. Wider fleets
  require coordinated quiescing or health-gated restart; otherwise the finite
  delivery budget can be exhausted and DLQ is the guaranteed outcome. This
  path is at-least-once and requires idempotent task effects.

## Topology and migration boundary

This candidate targets one logical ClickHouse service endpoint. It provides no
package-managed `ON CLUSTER` DDL, replica coordination, sharding, or
cross-replica result visibility contract. Those require a separate post-v0.1
architecture and compatibility gate.

Migration metadata is durable evidence, not a distributed lock. Every startup
validates actual physical schema even when the expected migration versions are
already present. For strict deployments, migrate once with the controlled
principal and run workers with `schema_mode="validate"` and least privilege.

## Evidence and handoff

The [user guide](user-guide.md) and [operator guide](operator-guide.md) define
the exact supported behavior.
The ordered [local release checklist](release-checklist.md) covers the canonical
source, service and clean-install package checks. The SHA-pinned Python-only
GitHub CI workflow expresses compatibility directly with native matrices and
does not upload raw worker evidence. Tagging, GitHub release creation and
package publication remain separate owner-authorized actions.
