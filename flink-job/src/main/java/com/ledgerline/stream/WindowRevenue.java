package com.ledgerline.stream;

import java.io.Serializable;

/** One-minute net revenue window. */
public class WindowRevenue implements Serializable {
    private static final long serialVersionUID = 1L;

    public long windowStart;
    public long windowEnd;
    public long revenueCents;
    public long eventCount;

    public WindowRevenue() {}

    public WindowRevenue(long windowStart, long windowEnd, long revenueCents, long eventCount) {
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.revenueCents = revenueCents;
        this.eventCount = eventCount;
    }
}
