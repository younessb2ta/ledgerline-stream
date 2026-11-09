# ADR-0004 — RocksDB with incremental checkpoints, not heap state

**Status:** Accepted · 2026-11-09

## Context

The job keeps two pieces of state that must survive a TaskManager loss:

- one running balance **per customer** (`ValueState<Long>`, keyed by
  `customer_id`);
- the open 1-minute windows, held for another 30 s of allowed lateness after
  their end.

The demo must survive `kill -9` on the TaskManager that hosts the job and
resume from a checkpoint with the same balances and the same window totals.
The machine has 8 GiB of RAM shared with Kafka, Postgres, the JobManager and a
second TaskManager, so state cannot simply live on the heap and grow.

Checkpoint storage is a local directory (`file://…/.checkpoints`) because this
is the zero-cost path; the production design uses object storage.

## Decision

Use Flink's **`EmbeddedRocksDBStateBackend` with incremental checkpoints**
(`new EmbeddedRocksDBStateBackend(true)`).

Checkpoint configuration:

| Setting | Value | Reason |
|---|---|---|
| interval | 10 s | short enough to bound replay after a kill |
| mode | `EXACTLY_ONCE` | consistent snapshot of state + Kafka offsets |
| min pause | 5 s | avoid back-to-back checkpoints under replay |
| timeout | 120 s | local disk, small state; a longer timeout hides problems |
| max concurrent | 1 | one consistent snapshot at a time |
| tolerable failures | 3 | transient disk/IO blips should not fail the job |
| externalized cleanup | `RETAIN_ON_CANCELLATION` | a demo can cancel and still restore |

Incremental checkpoints upload only changed SST files, so the 10 s interval is
cheap even as state grows. The end-to-end restore is at-least-once; the sink
makes replay harmless (ADR-0003).

## Alternatives considered

1. **Heap state (`HashMapStateBackend`).** Simplest, and fast for a few hundred
   customers. Rejected because state is bounded by the TaskManager heap, a
   checkpoint copies the whole map every 10 s, and the demo would stop being an
   honest rehearsal of a stateful production job.
2. **Full RocksDB checkpoints.** Correct, but rewrites all SSTs each time. With
   a 10 s interval the write amplification is unnecessary; incremental is the
   reason to choose RocksDB in the first place.
3. **External state store (Redis, Postgres) for balances.** Moves the
   consistency problem into the sink and loses the checkpoint/offset atomicity
   that makes the restore demo meaningful.

## Consequences

- Restore took **30.5 s** in the measured failover, including TaskManager
  restart, RocksDB open, and Kafka offset re-seek.
- Checkpoints are tied to the local disk. Losing that directory loses
  recoverability; the paid path replaces it with a bucket.
- The RocksDB native library must match the platform. Flink 1.20.3 ships it for
  macOS x86_64; a different architecture needs the matching artifact.
- State size and checkpoint duration are visible in the dashboard
  (`completed`, `failed`, `restored`, checkpoint age), which is what an operator
  needs during an incident.
