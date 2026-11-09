package com.ledgerline.stream;

import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Turns the window accumulator into a row for {@code revenue_windows}.
 *
 * <p>When an element arrives inside {@code allowedLateness}, Flink re-fires the
 * window and this function emits the same {@code window_start} again with a
 * larger total. The JDBC sink upserts on {@code window_start}, so the
 * provisional table is restated in place rather than double-counted.
 */
public class RevenueWindow
        extends ProcessAllWindowFunction<RevenueAcc, WindowRevenue, TimeWindow> {
    private static final long serialVersionUID = 1L;

    @Override
    public void process(Context context, Iterable<RevenueAcc> elements, Collector<WindowRevenue> out) {
        RevenueAcc acc = elements.iterator().next();
        TimeWindow window = context.window();
        out.collect(
                new WindowRevenue(window.getStart(), window.getEnd(), acc.revenueCents, acc.eventCount));
    }
}
