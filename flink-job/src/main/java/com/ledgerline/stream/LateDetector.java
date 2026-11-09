package com.ledgerline.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Splits events that are too late for their window into a side output.
 *
 * <p>The threshold is the operator's current watermark minus the allowed
 * lateness, which is the same rule Flink's window operator applies. Doing it
 * here rather than with {@code window.sideOutputLateData} is deliberate: the
 * watermark is available at this point, so {@code lateness_seconds} can be
 * measured in event time. Under an accelerated replay, event time and wall
 * clock run at different rates, so a wall-clock difference would be
 * meaningless.
 *
 * <p>Records that survive are re-timestamped downstream because a user
 * function does not forward the input timestamp.
 */
public class LateDetector extends ProcessFunction<Event, Event> {
    private static final long serialVersionUID = 1L;

    /** Kept in sync with {@code .allowedLateness(Time.seconds(30))}. */
    public static final long ALLOWED_LATENESS_MS = 30_000L;

    public static final OutputTag<LateEvent> LATE_TAG =
            new OutputTag<>("late-events", TypeInformation.of(LateEvent.class));

    private transient ObjectMapper mapper;

    @Override
    public void open(Configuration parameters) {
        mapper = new ObjectMapper();
    }

    @Override
    public void processElement(Event event, Context ctx, Collector<Event> out) throws Exception {
        long watermark = ctx.timerService().currentWatermark();
        long eventTs = event.eventTsMillis();

        if (watermark != Long.MIN_VALUE && eventTs <= watermark - ALLOWED_LATENESS_MS) {
            long latenessSeconds = Math.max(0L, (watermark - eventTs) / 1000L);
            ctx.output(
                    LATE_TAG,
                    new LateEvent(
                            event.eventId,
                            eventTs,
                            System.currentTimeMillis(),
                            latenessSeconds,
                            mapper.writeValueAsString(event)));
        } else {
            out.collect(event);
        }
    }
}
