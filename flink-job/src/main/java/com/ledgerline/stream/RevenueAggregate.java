package com.ledgerline.stream;

import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Sums signed money within an event-time window.
 *
 * <p>A refund is a negative amount, so a window's revenue is
 * {@code paid - refunded} without a second pass. Integer cents only.
 */
public class RevenueAggregate implements AggregateFunction<Event, RevenueAcc, RevenueAcc> {
    private static final long serialVersionUID = 1L;

    @Override
    public RevenueAcc createAccumulator() {
        return new RevenueAcc();
    }

    @Override
    public RevenueAcc add(Event event, RevenueAcc acc) {
        acc.revenueCents += event.signedAmountCents();
        acc.eventCount += 1;
        return acc;
    }

    @Override
    public RevenueAcc getResult(RevenueAcc acc) {
        return acc;
    }

    @Override
    public RevenueAcc merge(RevenueAcc left, RevenueAcc right) {
        left.revenueCents += right.revenueCents;
        left.eventCount += right.eventCount;
        return left;
    }
}
