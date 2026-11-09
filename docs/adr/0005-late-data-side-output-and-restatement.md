# ADR-0005 — Late data: side output and restatement, never a silent drop

**Status:** Accepted · 2026-11-09

## Context

Events carry an `event_ts` assigned by the producer, but arrival order is not
event order. Refunds are appended to the generator after the orders they
belong to, and can carry timestamps days earlier. A bounded replay also
compresses 14 days of event time into seconds.

The streaming view is **provisional** by design (ADR-0002). The question is
what to do with an event that arrives after the watermark has already passed
its window:

1. drop it silently — the ledger is quietly wrong;
2. keep the window open forever — unbounded state, no final answer;
3. accept it for a bounded time, then record it and restate.

## Decision

Use **event-time watermarks with bounded out-of-orderness and a custom late
detector**:

- `WatermarkStrategy.forBoundedOutOfOrderness(10 s)`.
- `LateDetector` compares each record's timestamp with
  `ctx.timerService().currentWatermark()`. If
  `event_ts <= watermark - 30 s` (the same rule as
  `.allowedLateness(30 s)`), the event goes to an `OutputTag<LateEvent>` side
  output.
- Lateness is recorded as `(watermark - event_ts) / 1000`, i.e. in **event
  time**. A wall-clock difference would be meaningless under an accelerated
  replay.
- Side-output rows go to `late_events` with the full original payload.
- The main stream only carries accepted events: late ones never reach
  `customer_balances` or `revenue_windows`.
- `ledger_events` still records **every** event, late or not. It is the
  replayable log and the input to restatement.
- `reconciliation --restate` deletes and recomputes
  `revenue_windows_restated` from all of `ledger_events`. Running it twice is
  idempotent.

Events inside the 30 s allowance are handled by Flink itself: the window
re-fires and `revenue_windows` upserts on `window_start`, so the provisional
row is restated in place rather than double-counted.

## Alternatives considered

1. **`window.sideOutputLateData`.** Correctly captures elements the window
   operator considers late, but it does not expose the event-time gap, so
   `lateness_seconds` would have to be reconstructed later. The custom detector
   runs where the watermark is available and is explicit about the rule.
2. **Infinite allowed lateness.** Every late event restates a window, state
   grows without bound, and the provisional view never converges.
3. **Drop late events.** Rejected: a ledger that silently discards money is
   worse than one that admits it is provisional.
4. **Let the batch layer own all corrections.** It does (ADR-0002), but waiting
   a day to acknowledge an event that already arrived is a worse operator
   experience. The demo restates on demand.

## Consequences

- A late event is visible in three places with three meanings:
  `ledger_events` (it happened), `late_events` (it missed its window, with the
  measured lateness), and — after restatement — `revenue_windows_restated`
  (the corrected total).
- Provisional totals plus the signed sum of late events equal the restated
  total. The reconciliation report checks both sides.
- The 30 s allowance is a policy choice, not a technical limit. It is kept in
  sync between `LateDetector.ALLOWED_LATENESS_MS` and
  `.allowedLateness(Time.seconds(30))`; changing one without the other would
  make the two definitions disagree.
- Lateness is measured against the *operator's* watermark at the moment the
  record is processed, which depends on checkpoint/restore timing. The report
  records the value observed in that run rather than a theoretical constant.
- Restatement is a full recompute. That is fine for one day and honest for a
  demo; a production version would restrict it to affected windows.
