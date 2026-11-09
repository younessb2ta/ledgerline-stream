# Architecture decisions in this repository

The platform-level ADRs live in `ledgerline-batch/docs/adr/`; the two below are
specific to the streaming path.

| ADR | Decision | Status |
|---|---|---|
| [0004](0004-rocksdb-incremental-checkpoints.md) | RocksDB + incremental checkpoints over heap state | Accepted |
| [0005](0005-late-data-side-output-and-restatement.md) | Late data: side output + restatement, not a silent drop | Accepted |

Shared with `ledgerline-batch`:

- ADR-0001 — GCP + BigQuery sandbox, not a billed project
- ADR-0002 — process the same events twice: provisional stream, authoritative batch
- ADR-0003 — exactly-once via at-least-once + idempotent sink, not 2PC
- ADR-0006 — expectations before load, dbt tests after transform
- ADR-0007 — trailing-average volume band
- ADR-0008 — GitHub Actions + local Airflow instead of Composer
- ADR-0009 — dbt models are tables/views only
