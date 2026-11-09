# Deterministic replay

The streaming demos compare a live pipeline against `eventforge`'s ground
truth. That comparison is only meaningful if replaying the same seed produces
the same projection. Three decisions make the run deterministic; each one was
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

## 3. Close the replay with an explicit marker

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

With those three rules the same seed always yields the same artifact:

```
events: 2,870 (2,870 distinct), 396 balances, 632 windows, 0 late
balance total = window total = 39,953,895 cents
event_id sha256 = 1a63685fd48c191a…
```

The full day's replay takes about 20 s at `--rate 600`, including the marker
and the watermark advance.
