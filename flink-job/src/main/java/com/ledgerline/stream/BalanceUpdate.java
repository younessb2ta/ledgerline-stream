package com.ledgerline.stream;

import java.io.Serializable;

/** Emitted after every money event: the new running balance for a customer. */
public class BalanceUpdate implements Serializable {
    private static final long serialVersionUID = 1L;

    public String customerId;
    public long balanceCents;
    public String eventId;
    public long eventTs;

    public BalanceUpdate() {}

    public BalanceUpdate(String customerId, long balanceCents, String eventId, long eventTs) {
        this.customerId = customerId;
        this.balanceCents = balanceCents;
        this.eventId = eventId;
        this.eventTs = eventTs;
    }
}
