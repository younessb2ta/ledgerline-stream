package com.ledgerline.stream;

import java.io.Serializable;

/**
 * An event that missed its window by more than the allowed lateness.
 *
 * <p>It is written to {@code late_events} instead of being dropped, so the
 * restatement job can add it back. {@code latenessSeconds} is measured in
 * event time against the operator's watermark, not against the wall clock:
 * under an accelerated replay those two clocks run at different rates.
 */
public class LateEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    public String eventId;
    public long eventTs;
    public long arrivalTs;
    public long latenessSeconds;
    public String payload;

    public LateEvent() {}

    public LateEvent(String eventId, long eventTs, long arrivalTs, long latenessSeconds, String payload) {
        this.eventId = eventId;
        this.eventTs = eventTs;
        this.arrivalTs = arrivalTs;
        this.latenessSeconds = latenessSeconds;
        this.payload = payload;
    }

    @Override
    public String toString() {
        return "LateEvent{" + eventId + " lateness=" + latenessSeconds + "s}";
    }
}
