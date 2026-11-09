# Evidence

Copies of the reports produced by the two demos and by a clean replay. The
live files are written to `artifacts/` (gitignored); these are curated so the
numbers in the README can be inspected without rerunning the stack.

| File | Produced by | What it shows |
|---|---|---|
| `exactly-once-20261109T163222Z.json` | `make demo-exactly-once` | TaskManager killed after checkpoint 1, restored in 30.5 s, 239 replayed writes absorbed, 2,870 distinct events, totals equal on both sinks |
| `late-data-20261109T163432Z.json` | `make demo-late-data` | 13 late events, provisional 39,821,419 cents, restated +132,476 cents across 3 windows, restatement checksum repeated identically |
| `late-data-20261109T163432Z-rerun.json` | `make restate` | Same 3-window diff recomputed from `ledger_events`; no drift |
| `reconciliation-clean-20261109T165046Z.json` | `make replay` | 2,870 events / 396 balances / 632 windows / 0 late; every check PASS |

Reproduce with the commands in the top-level README. The generator inputs are
fixed (seed 42, start 2026-11-01, 1 day), so the event-ID hash
`1a63685fd48c191a…` and the totals should not change.
