package com.ledgerline.stream;

import java.io.Serializable;

/** Accumulator for the window aggregate. */
public class RevenueAcc implements Serializable {
    private static final long serialVersionUID = 1L;

    public long revenueCents;
    public long eventCount;

    public RevenueAcc() {}
}
