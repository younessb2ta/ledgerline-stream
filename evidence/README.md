# Evidence

Copies of the reports produced by the two demos and by a clean replay. The
live files are written to `artifacts/` (gitignored); these are curated so the
numbers in the README can be inspected without rerunning the stack.

| File | Produced by | What it shows |
|---|---|---|
| `exactly-once-20261109T170920Z.json` | `make demo-exactly-once` | TaskManager killed after checkpoint 1, restored in 21.3 s, 182 replayed writes absorbed, 2,870 distinct events, totals equal on both sinks |
| `exactly-once-20261111T180210Z.json` | `make demo-exactly-once` (second run) | Kill landed later: 27.6 s restore, 3,211 attempts, 341 replayed. Same 2,870 distinct events, zero duplicates, same totals — the replay count is timing-dependent, the invariants are not |
| `late-data-20261109T170802Z.json` | `make demo-late-data` | 141 late events, provisional 37,899,938 cents, restated +2,053,957 cents across 34 windows, restatement checksum repeated identically |
| `late-data-20261109T170802Z-rerun.json` | `make restate` | Same 34-window diff recomputed from `ledger_events`; no drift |
| `reconciliation-clean-20261109T171151Z.json` | `make replay` | 2,870 events / 396 balances / 632 windows / 0 late; every check PASS |

Reproduce with the commands in the top-level README. The generator inputs are
fixed (seed 42, start 2026-11-01, 1 day), so the event-ID hash
`1a63685fd48c1932…` and the totals should not change. Two consecutive
late-data runs produced the same 141 late events, the same provisional total,
and the same restatement checksum `556e9e78…`.

Within the exactly-once report, restore time and the number of replayed writes
depend on how much of the producer ran before the kill. The invariant is the
row count (2,870 distinct), zero duplicates, and equal totals on both sinks.
The second committed run absorbed 341 writes instead of 182 and passed the same
checks.
