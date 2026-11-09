package com.ledgerline.stream;

import org.apache.flink.api.common.functions.FilterFunction;

/** Only paid and refunded events move money; created/cancelled do not. */
public class IsMoneyEvent implements FilterFunction<Event> {
    private static final long serialVersionUID = 1L;

    @Override
    public boolean filter(Event event) {
        return event.isMoneyEvent();
    }
}
