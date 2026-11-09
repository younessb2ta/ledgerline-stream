package com.ledgerline.stream;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.time.Instant;

/**
 * One commerce event, matching eventforge's JSON contract.
 *
 * <p>Public fields + no-arg constructor keep this a Flink POJO, so Flink can
 * serialize it without Kryo and the type is inspectable in the UI.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Event implements Serializable {
    private static final long serialVersionUID = 1L;

    @JsonProperty("event_id")
    public String eventId;

    @JsonProperty("event_type")
    public String eventType;

    @JsonProperty("event_ts")
    public String eventTs;

    @JsonProperty("order_id")
    public String orderId;

    @JsonProperty("customer_id")
    public String customerId;

    @JsonProperty("product_id")
    public String productId;

    @JsonProperty("quantity")
    public int quantity;

    @JsonProperty("unit_price_cents")
    public long unitPriceCents;

    @JsonProperty("amount_cents")
    public long amountCents;

    @JsonProperty("currency")
    public String currency;

    @JsonProperty("schema_version")
    public int schemaVersion;

    public Event() {}

    public long eventTsMillis() {
        return Instant.parse(eventTs).toEpochMilli();
    }

    /** Only paid/refunded events move money. */
    public boolean isMoneyEvent() {
        return "order_paid".equals(eventType) || "refund_issued".equals(eventType);
    }

    /** A refund subtracts; everything else that reaches here adds. */
    public long signedAmountCents() {
        return "refund_issued".equals(eventType) ? -amountCents : amountCents;
    }

    @Override
    public String toString() {
        return "Event{" + eventId + " " + eventType + " " + eventTs + " customer=" + customerId + "}";
    }
}
