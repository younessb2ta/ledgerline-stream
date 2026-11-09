package com.ledgerline.stream;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Keyed running balance. The state is what makes this a stateful job: Flink
 * keeps one {@code Long} per customer key in RocksDB, and a checkpoint captures
 * it together with the Kafka offsets.
 *
 * <p>Updates are emitted as absolute values, not increments, so replaying a
 * checkpoint re-applies the same value instead of adding it again.
 */
public class BalanceFunction extends KeyedProcessFunction<String, Event, BalanceUpdate> {
    private static final long serialVersionUID = 1L;

    private transient ValueState<Long> balance;

    @Override
    public void open(Configuration parameters) {
        balance = getRuntimeContext().getState(new ValueStateDescriptor<>("balance-cents", Long.class));
    }

    @Override
    public void processElement(Event event, Context ctx, Collector<BalanceUpdate> out) throws Exception {
        if (!event.isMoneyEvent()) {
            return;
        }
        Long current = balance.value();
        long next = (current == null ? 0L : current) + event.signedAmountCents();
        balance.update(next);
        out.collect(new BalanceUpdate(event.customerId, next, event.eventId, event.eventTsMillis()));
    }
}
