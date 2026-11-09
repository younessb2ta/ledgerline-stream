package com.ledgerline.stream;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/**
 * Bounded out-of-orderness watermarks emitted once per event.
 *
 * <p>Flink's built-in {@code forBoundedOutOfOrderness} emits periodically (every
 * 200 ms by default). In an accelerated replay that interval is a wall-clock
 * quantity: a record 300 s behind the newest event is only classified as late
 * if it happens to be processed after the next periodic emission. The same
 * seed then produced 13 late events on one run and 16 on the next, so the
 * late-data evidence was unreproducible.
 *
 * <p>Emitting on every event ties the watermark to the input order instead:
 * after event <i>i</i> it is {@code max_ts(0..i) - outOfOrderness}. The window
 * operator then applies the same rule on every replay, regardless of how fast
 * the producer runs.
 *
 * <p>The cost is one watermark per record, which is fine at this scale (one
 * Kafka partition, parallelism 1). A high-rate production job would emit on a
 * periodic tick or every N records instead.
 */
public class EventDrivenWatermarks implements WatermarkGenerator<Event> {
    private final long outOfOrdernessMillis;
    private long maxTimestamp = Long.MIN_VALUE;

    public EventDrivenWatermarks(long outOfOrdernessMillis) {
        this.outOfOrdernessMillis = outOfOrdernessMillis;
    }

    @Override
    public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {
        maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
        output.emitWatermark(new Watermark(maxTimestamp - outOfOrdernessMillis));
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        // Watermarks follow the events; nothing to do on the periodic tick.
    }
}
