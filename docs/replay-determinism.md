# Deterministic replay

The streaming demos compare a live pipeline against `eventforge`'s ground
truth. That comparison is only meaningful if replaying the same seed produces
the same projection. Four decisions make the run deterministic; each one was
forced by an observed failure, not chosen up front.

## 1. Produce in event-time order

`eventforge` generates a day of orders, then appends refunds. A refund can
carry a timestamp days earlier than the record before it. Produced in
generator order (`--no-shuffle`), that looks like real lateness to a 10 s
watermark, and the reconciliation report flagged events as late that were
merely out of order.

`--sort` orders the full set by `(event_ts, event_id)` before producing.
Sorting changes order, never membership, so the ground truth is unchanged. It
also implies `--no-shuffle`:

```bash
uv run eventforge kafka --seed 42 --start 2026-11-01 --days 1 \
  --bootstrap 127.0.0.1:9092 --topic ledger.events --rate 600 --sort
```

## 2. One partition, parallelism 1

With three partitions, the Flink source reads them independently. A fetch batch
from partition A can be minutes ahead of partition B in event time; the global
watermark is the minimum, so records from A are declared late against a
watermark driven by B. The demo reported lateness that did not exist.

Raising parallelism introduced a second problem: a rebalance before `keyBy`
sends updates for the same customer over different network channels, so the
absolute balance writes can land out of order. The final value is then the
second-to-last update.

The replay therefore uses:

- a **single-partition Kafka topic** (`stack.kafka.reset_topic` defaults to 1);
- **job parallelism 1**, including the source (`source-parallelism=1`).

One partition gives a total order, so event time and arrival time agree.
Scaling out is a separate design: partition by `customer_id`, keep the
source-to-`keyBy` chain aligned, and accept per-key windows.

An idle source subtask is also why parallelism must not exceed the partition
count: a subtask with no splits never emits a watermark and holds the
downstream minimum at `Long.MIN_VALUE`.

## 3. Emit watermarks from events, not the wall clock

Flink's `forBoundedOutOfOrderness(10 s)` emits on a periodic tick (every 200 ms
by default). Under an accelerated replay that interval is a *wall-clock*
quantity: whether a record 300 s behind the newest event is classified as late
depends on whether it happens to be processed before or after the next tick.
The same seed produced **13 late events on one run and 16 on the next**, so the
late-data evidence was not reproducible even though the input was identical.

`EventDrivenWatermarks` emits once per record instead: after event *i* the
watermark is `max_ts(0..i) − 10 s`. Late classification becomes a function of
input order alone, independent of producer rate or tick timing. One watermark
per record is cheap at this scale (one partition, parallelism 1); a high-rate
production job would emit on a tick or every N records.

`eventforge.apply_lateness` is the matching change on the producer side: a
selected record is stamped `late_seconds` behind the **newest event produced so
far**, not behind its own timestamp. The stream is sparse enough that an event
300 s older than itself can still be newer than its predecessor and therefore
not late. Anchoring to the newest event makes every selected record
unambiguously late (290 s under the 10 s bound) and keeps the set fixed.

## 4. Close the replay with an explicit marker

Kafka never signals end-of-input. A tumbling window fires only when the
watermark crosses its end, so a bounded replay leaves the final window open.
Measured through the REST API, the watermark stayed at `max_event_ts − 10 s`
indefinitely. `withIdleness(30 s)` did not advance it.

`uv run python -m stack.kafka flush` produces a `stream_flush` control record
with `event_ts = 9999-12-31T23:59:59Z`, **one per partition**:

- it is a normal JSON record, so `JsonToEvent` and both timestamp assigners
  process it;
- the first assigner is before `LateDetector`, the second is after it (a
  `ProcessFunction` does not forward timestamps), so two assignments are needed;
- `IsLedgerEvent` and `IsMoneyEvent` filter it out before every sink, so it
  never appears in `ledger_events`, balances, or windows;
- one marker per partition matters: the job watermark is the minimum across
  partitions.

Send it only after the last real event. Anything produced afterwards is late by
definition.

## Result

With those four rules the same seed always yields the same artifact:

```
clean replay:  events: 2,870 (2,870 distinct), 396 balances, 632 windows, 0 late
balance total = window total = 39,953,895 cents
event_id sha256 = 1a63685fd48c1932…

late-data run: 141 late, 611 provisional windows, 37,899,938 cents
restated:      +2,053,957 cents across 34 windows, checksum 556e9e78…
```

Two consecutive late-data runs produced identical counts and checksums. The
full day's replay takes about 20 s at `--rate 600`, including the marker and the
watermark advance.
