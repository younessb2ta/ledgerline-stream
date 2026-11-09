package com.ledgerline.stream;

import org.apache.flink.api.common.functions.FilterFunction;

/**
 * Keeps replay traffic out of the ledger table.
 *
 * <p>{@code stream_flush} is not a commerce event. It exists only to carry a
 * far-future timestamp through the watermark assigners so that every window
 * still open at the end of a bounded replay is closed. It is dropped before
 * the sink, so ground truth stays exactly what {@code eventforge} generated.
 */
public class IsLedgerEvent implements FilterFunction<Event> {
    private static final long serialVersionUID = 1L;

    public static boolean isControl(Event event) {
        return "stream_flush".equals(event.eventType);
    }

    @Override
    public boolean filter(Event event) {
        return !isControl(event);
    }
}
