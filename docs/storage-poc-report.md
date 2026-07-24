# TASK-017 storage and client POC report

Date: 2026-07-15

This report freezes the empirical inputs for schema migration v1. The source of
truth remains the executable suite under `tests/integration`; every run writes
version-attributed JSON observations, raw `EXPLAIN` text and JUnit XML under
`.test-artifacts/`.

## Compatibility matrix

The final matrix ran 26 required tests in every cell with no skip or xfail.

| ClickHouse server | `clickhouse-connect` | Result |
| --- | --- | --- |
| 25.8.28.1 | 1.4.2 | 26 passed |
| 25.8.28.1 | 1.5.0 | 26 passed |
| 26.6.1.1193 | 1.4.2 | 26 passed |
| 26.6.1.1193 | 1.5.0 | 26 passed |

Candidate minimum server image:
`clickhouse/clickhouse-server:25.8.28.1@sha256:a9d328123ff8a61bf6b16448528b577d59deb85758172e13b09054b0727f8adf`.

Current comparison image:
`clickhouse/clickhouse-server:26.6.1.1193@sha256:1d1f6508eba2dccce2cee9913907c5f7766327debc57a6b1991f2c9e3176c163`.

The supported boundaries are ClickHouse 25.8.28.1 or later within the tested
single-endpoint contract and `clickhouse-connect>=1.4.2,<2`. Version 1.5.0 was
the current client boundary at the time of this POC. The exact 1.5.0 rows above
are historical evidence, not a permanent resolver pin or a claim that the same
artifact will remain available from the public index.

## Frozen decisions

- Payload and log columns remain ClickHouse `String`. Explicit bytes formats
  preserve empty values, NUL, invalid UTF-8, all 256 octets and a 3 MiB payload
  through direct and aliased projections; base64 is not required.
- Time columns remain `DateTime64(6, 'UTC')`. The application-supported range is
  1900-01-01 through 2299-12-31 23:59:59.999999 UTC. Bind, insert and deadline
  overflow into 2300 are rejected before a logical write.
- `written_at` comes from the server. `generation_at` is allocated one
  microsecond later than the maximum of server time and stored history, so a
  simulated server-clock rollback cannot reorder generations.
- The result sorting key is
  `(namespace, task_id, generation_at, generation_id, state)`; the progress key
  omits only `state`. Latest queries use the complete reverse key, including
  fixed `namespace DESC, task_id DESC`. On both servers the plans use primary-key
  binary search and `ReadPoolInOrder` with `InReverseOrder`.
- Latest selection happens before state and visibility interpretation. Result,
  targeted tombstone and progress ordering remains correct across different
  `purge_at` partitions. A tombstone for generation A does not hide newer B.
- The physical schema uses `PARTITION BY toYYYYMM(purge_at)`, primary key
  `(namespace, task_id)` and one table TTL on `purge_at`. `system.columns`,
  `DESCRIBE TABLE` and exact normalized `engine_full` are startup validation
  inputs. The observed `index_granularity = 8192` is a server default, not a
  performance-tuning decision.
- Result writes, tombstones, progress and metadata use synchronous inserts in
  v0.1 with `async_insert=0`, `wait_for_async_insert=1` and
  `wait_end_of_query=1`. Acknowledged async inserts were immediately visible,
  but their cancellation has ambiguous delivery and the two selected servers
  have different `async_insert` defaults. There is no proven benefit that
  justifies making that queue part of the baseline contract.
- `wait_for_async_insert=0` is rejected. The call returned while its row was
  pending, the later constraint failure was not returned to the caller and the
  row was absent. Synchronous, acknowledged-async, immediate DDL and late DDL
  failures were surfaced as definite `DatabaseError` instances.
- A lost insert response can trigger a driver retry and produce two physical
  copies of the same frozen identity. The bounded protocol therefore confirms
  the exact identity before retrying, reuses that identity for at most one
  retry, and handles present, absent-then-present, final-absent and confirmation
  failure branches without allocating a new logical generation.
- `OperationalError` is ambiguous transport I/O. `StreamFailureError` is also
  ambiguous and must be classified explicitly: it inherits directly from
  `Exception`, occurs after successful Native-response headers, and is not
  retried by the driver even when `query_retries=2`. Auth, constraint, schema
  and programming failures remain definite.
- External cancellation and timeout propagate. Cancelling raw client creation
  after session allocation leaks the session unless package startup owns and
  awaits the factory task. Cancelling raw `close()` during lease drain loses the
  lease reference and a second raw close cannot recover it. Package startup and
  shutdown must therefore use owned shielded tasks and await terminal cleanup
  before propagating outer cancellation.
- Literal IPv6 hosts are rejected in v0.1. aiohttp requires a bracketed URL, but
  the tested driver compares that bracketed host literally against `NO_PROXY`:
  the standard bracket-free IPv6 entry does not bypass a proxy. DNS hostnames
  that resolve to IPv6 are not affected by this restriction.
- `with_logs=False` selects only `result_payload`; direct and aliased queries do
  not return `log_payload`. Compact and Wide parts were both observed. This POC
  proves SQL/network projection only and deliberately makes no physical-I/O
  claim; performance qualification remains deployment-specific and outside CI.

## Rejected variants

- Base64 payload storage: unnecessary expansion after lossless raw-byte proof.
- Visibility filtering before latest selection: resurrects an older result.
- Short latest `ORDER BY generation_at, generation_id, ...`: does not establish
  the selected minimum server's complete reverse read-order contract.
- Fire-and-forget inserts: acknowledge queue acceptance rather than outcome.
- Acknowledged async inserts as the v0.1 default: correct when fully configured,
  but add queue and cancellation ambiguity without a demonstrated gain.
- Treating only `OperationalError` as ambiguous: misses partial Native streams.
- Retrying cancellation: an acknowledged async insert committed after its
  awaiting task received `CancelledError`.
- Literal IPv6 host support: deterministic standard `NO_PROXY` behavior was not
  available through the selected driver interface.
- Inferring physical log-column I/O savings from a narrow projection: not
  established for both Compact and Wide parts.

## Reproduction

GitHub Actions owns the current four-cell ClickHouse-server/client matrix
directly. This report records the historical POC matrix that established the
boundary; the repository does not maintain a second Python orchestrator for
that matrix. The integration suite itself remains under `tests/integration`.
