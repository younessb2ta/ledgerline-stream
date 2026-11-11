# Demo video — 3 minutes

A recording guide for `ledgerline-stream`. The four claims to prove: a clean
replay reconciles to the generator, a killed TaskManager loses no money, late
events are restated, and the dashboard shows the provisional view.

## Before recording

```bash
make install
make build
make stack-up
make stack-status          # 2 taskmanagers, Postgres up, Kafka up
make dashboard             # leave running at http://127.0.0.1:8501
```

Keep these windows open:

| Window | Content |
|---|---|
| Terminal A (large font) | commands below |
| Terminal B | `make dashboard` (or the browser tab) |
| Browser | Flink UI at `http://127.0.0.1:8081`, checkpoints tab |
| Editor | `../ledgerline-batch/docs/adr/0003-at-least-once-idempotent-sink.md`, `docs/replay-determinism.md`, `evidence/README.md` |

Recording setup: 1080p or 1440p, terminal ≥ 16 pt, notifications off. The
failure demos take minutes; record live but keep the cuts honest — show the
kill, the restore, and the final numbers rather than skipping the wait in a way
that implies it was instant.

## Scenes

| Time | On screen | Voiceover |
|---|---|---|
| 0:00–0:20 | Title card: **Ledgerline — streaming path**. Then the parent README seam table. | "The same events, processed twice. Batch is the authoritative warehouse; this path is the provisional ledger, and its job is to be fast without losing money." |
| 0:20–0:40 | Architecture fence from the README; scroll Flink UI's job graph. | "Kafka carries one ordered topic. Flink keeps RocksDB state, checkpoints to disk, and writes to Postgres with an idempotent upsert. Streamlit reads Postgres directly." |
| 0:40–1:10 | `make replay`. Show reset, submit, 2,870 events, flush, settle. Then the reconciliation table. | "One command resets state, submits job parallelism one, produces a deterministic day, sends an end-of-replay marker, waits for a checkpoint, and reconciles. Every check passes: 2,870 distinct events, 396 balances, 632 windows." |
| 1:10–1:25 | Dashboard: revenue, late count, checkpoint health. | "The dashboard is the operator view: revenue in cents, the open-window count, and whether checkpoints are healthy. For this clean replay the total is $399,538.95." |
| 1:25–2:05 | `make demo-exactly-once`. Show the TaskManager kill, Flink restart, restore time, and the insert-attempt table. | "Now the failure proof. The demo kills a TaskManager after the first checkpoint. Flink restores from RocksDB and the Kafka offsets in that checkpoint, replays 182 writes, and the sink's `ON CONFLICT` absorbs them. 3,052 attempts for 2,870 events, zero duplicates, totals unchanged. That is at-least-once plus an idempotent sink — effectively-once, not two-phase commit." |
| 2:05–2:40 | `make demo-late-data`. Show provisional dashboard, then the restatement diff. | "Now 5% of events arrive 290 seconds late. The provisional view excludes 141 events and shows $378,999.38. Restatement adds $20,539.57 across 34 windows and lands on the same $399,538.95. The checksum is identical on a rerun, so the correction is repeatable, not a one-off." |
| 2:40–3:00 | `evidence/` index, then `docs/replay-determinism.md` rules 1–4. | "The curated reports ship with the repo. Determinism is four rules: one partition, parallelism one, event-driven watermarks, and a flush marker that closes the last window. Scale-out is deliberately out of scope and documented as such." |

## Numbers to say correctly

- Clean replay: 2,870 events, 396 balances, 632 windows, 0 late,
  39,953,895 cents = $399,538.95; event-ID hash `1a63685fd48c1932…`.
- Exactly-once: kill after checkpoint 1; restore 21.3 s; 3,052 insert attempts;
  182 replayed and absorbed; 0 duplicate rows; final total 39,953,895 cents.
- Late data: 141 events, 290 s late; provisional 37,899,938 cents =
  $378,999.38; restated +2,053,957 cents across 34 windows; checksum
  `556e9e78fd29d1c87b4485d2e86a60fe`.
- State: RocksDB, incremental checkpoints every 10 s, local filesystem target.

## Claims to keep precise

- Say **effectively-once**: at-least-once delivery plus an idempotent sink. Do
  not claim a distributed transaction.
- Say **provisional and restatable**, not "real-time truth".
- Say **parallelism one and one partition** when explaining replay determinism;
  the README already lists scale-out as a limitation.
- The dashboard is an operator view over Postgres, not a serving layer.

## If you only have 90 seconds

Keep the clean reconciliation table, 20 seconds of the kill/restore, and the
provisional-versus-restated total. Cut the architecture and dashboard scenes to
one sentence each. Never cut the zero-duplicates line or the restatement
checksum.
