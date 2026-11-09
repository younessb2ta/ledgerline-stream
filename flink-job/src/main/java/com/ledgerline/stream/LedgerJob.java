package com.ledgerline.stream;

import java.time.OffsetDateTime;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Ledgerline streaming job.
 *
 * <pre>
 * Kafka (ledger.events)
 *   └─ JSON -> Event
 *       ├─ ledger_events            (every event, ON CONFLICT DO NOTHING)
 *       └─ watermarks (10 s bounded, emitted per event, end-of-replay flush)
 *           ├─ late detector  ── side output ── late_events
 *           └─ money events
 *               ├─ keyBy(customer) -> ValueState balance -> customer_balances
 *               └─ 1-minute tumbling window (30 s allowed lateness) -> revenue_windows
 * </pre>
 *
 * <p>State lives in RocksDB; checkpoints run every 10 s and are retained on
 * cancellation. Recovery is at-least-once + idempotent sink, never two-phase
 * commit — see {@code docs/adr/0003}.
 */
public class LedgerJob {
    /** Kept in sync with the watermark bound used by {@link EventDrivenWatermarks}. */
    private static final long OUT_OF_ORDERNESS_MS = 10_000L;

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);

        String bootstrap = params.get("bootstrap", "127.0.0.1:9092");
        String topic = params.get("topic", "ledger.events");
        String group = params.get("group", "ledgerline-stream");
        String jdbcUrl = params.get("jdbc-url", "jdbc:postgresql://127.0.0.1:5433/ledgerline");
        String jdbcUser = params.get("jdbc-user", "postgres");
        String jdbcPassword = params.get("jdbc-password", "");
        String checkpointDir = params.get("checkpoints", "file:///tmp/ledgerline-checkpoints");
        int sourceParallelism = params.getInt("source-parallelism", 1);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Parallelism 1 on purpose. The replay is ordered by event time, and a
        // rebalance before `keyBy` would let two network channels reorder
        // updates for the same customer. It would also split the end-of-replay
        // marker across watermark assigners, leaving one subtask at the old
        // watermark and the last window unclosed. Scaling out is a separate
        // exercise: Kafka would have to be partitioned by customer_id and the
        // source-to-keyBy chain kept aligned.
        env.setParallelism(1);
        env.getConfig().setGlobalJobParameters(params);

        // --- fault tolerance -------------------------------------------------
        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
        env.enableCheckpointing(TimeUnit.SECONDS.toMillis(10), CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig checkpoints = env.getCheckpointConfig();
        checkpoints.setCheckpointStorage(new FileSystemCheckpointStorage(checkpointDir));
        checkpoints.setMinPauseBetweenCheckpoints(5_000);
        checkpoints.setCheckpointTimeout(120_000);
        checkpoints.setMaxConcurrentCheckpoints(1);
        checkpoints.setTolerableCheckpointFailureNumber(3);
        checkpoints.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // --- source ----------------------------------------------------------
        KafkaSource<String> source =
                KafkaSource.<String>builder()
                        .setBootstrapServers(bootstrap)
                        .setTopics(topic)
                        .setGroupId(group)
                        .setStartingOffsets(
                                OffsetsInitializer.committedOffsets(
                                        org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST))
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .setProperty("partition.discovery.interval.ms", "10000")
                        .build();

        DataStream<String> raw =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source")
                        // One subtask per configured split. With the default
                        // single-partition replay topic this must be 1: an idle
                        // subtask that never assigns a watermark would hold the
                        // downstream window watermark at Long.MIN_VALUE.
                        .setParallelism(sourceParallelism);

        DataStream<Event> events = raw.map(new JsonToEvent()).name("json-to-event");

        JdbcExecutionOptions execution =
                JdbcExecutionOptions.builder()
                        .withBatchSize(200)
                        .withBatchIntervalMs(1_000)
                        .withMaxRetries(3)
                        .build();
        JdbcConnectionOptions connection =
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(jdbcUrl)
                        .withDriverName("org.postgresql.Driver")
                        .withUsername(jdbcUser)
                        .withPassword(jdbcPassword)
                        .build();

        // Every real event is recorded, including the late ones. This table is
        // the replayable projection the restatement job reads. Replay control
        // markers are filtered out of this branch but still flow through the
        // watermark assigners below.
        events
                .filter(new IsLedgerEvent())
                .name("ledger-only")
                .addSink(
                        JdbcSink.sink(
                                "INSERT INTO ledger_events (event_id, event_type, event_ts, order_id,"
                                        + " customer_id, product_id, quantity, unit_price_cents,"
                                        + " amount_cents, currency, schema_version)"
                                        + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                                        + " ON CONFLICT (event_id) DO NOTHING",
                                (statement, event) -> {
                                    statement.setString(1, event.eventId);
                                    statement.setString(2, event.eventType);
                                    statement.setObject(3, OffsetDateTime.parse(event.eventTs));
                                    statement.setString(4, event.orderId);
                                    statement.setString(5, event.customerId);
                                    statement.setString(6, event.productId);
                                    statement.setInt(7, event.quantity);
                                    statement.setLong(8, event.unitPriceCents);
                                    statement.setLong(9, event.amountCents);
                                    statement.setString(10, event.currency);
                                    statement.setInt(11, event.schemaVersion);
                                },
                                execution,
                                connection))
                .name("ledger-events-sink");

        // --- event time and late data ----------------------------------------
        // A bounded replay needs an explicit end marker: a tumbling window only
        // fires once the watermark crosses its end, and a Kafka source never
        // says "done". `stack.kafka flush` publishes a `stream_flush` event with
        // a far-future timestamp; it advances the watermark through this
        // operator and is filtered out before any sink. Relying on
        // `withIdleness` was not enough here: measured over REST, the watermark
        // stayed at `max_event_ts - 10s` indefinitely.
        // Emitted per event rather than on Flink's 200 ms periodic tick: a
        // wall-clock interval makes late classification depend on replay speed.
        // See EventDrivenWatermarks for the measured 13-vs-16 failure.
        WatermarkStrategy<Event> watermarks =
                WatermarkStrategy.<Event>forGenerator(
                                context -> new EventDrivenWatermarks(OUT_OF_ORDERNESS_MS))
                        .withTimestampAssigner((event, timestamp) -> event.eventTsMillis());

        SingleOutputStreamOperator<Event> onTime =
                events.assignTimestampsAndWatermarks(watermarks)
                        .process(new LateDetector())
                        .name("late-detector");

        onTime.getSideOutput(LateDetector.LATE_TAG)
                .addSink(
                        JdbcSink.sink(
                                "INSERT INTO late_events (event_id, event_ts, arrival_ts,"
                                        + " lateness_seconds, payload)"
                                        + " VALUES (?, to_timestamp(? / 1000.0),"
                                        + " to_timestamp(? / 1000.0), ?, ?::jsonb)"
                                        + " ON CONFLICT (event_id) DO NOTHING",
                                (statement, late) -> {
                                    statement.setString(1, late.eventId);
                                    statement.setLong(2, late.eventTs);
                                    statement.setLong(3, late.arrivalTs);
                                    statement.setLong(4, late.latenessSeconds);
                                    statement.setString(5, late.payload);
                                },
                                execution,
                                connection))
                .name("late-events-sink")
                .setParallelism(1);

        // A user function does not forward timestamps, so re-assign them here.
        DataStream<Event> timed =
                onTime.assignTimestampsAndWatermarks(watermarks).name("watermarks");
        DataStream<Event> money = timed.filter(new IsMoneyEvent()).name("money-only");

        // --- stateful running balance ----------------------------------------
        money.keyBy(event -> event.customerId)
                .process(new BalanceFunction())
                .name("customer-balance")
                .addSink(new BalanceSink(jdbcUrl, jdbcUser, jdbcPassword))
                .name("balance-sink")
                .setParallelism(1);

        // --- 1-minute revenue windows ----------------------------------------
        SingleOutputStreamOperator<WindowRevenue> revenue =
                money.windowAll(TumblingEventTimeWindows.of(Time.minutes(1)))
                        .allowedLateness(Time.seconds(30))
                        .aggregate(new RevenueAggregate(), new RevenueWindow())
                        .name("revenue-window");

        revenue
                .addSink(
                        JdbcSink.sink(
                                "INSERT INTO revenue_windows (window_start, window_end,"
                                        + " revenue_cents, event_count, updated_at)"
                                        + " VALUES (to_timestamp(? / 1000.0),"
                                        + " to_timestamp(? / 1000.0), ?, ?, now())"
                                        + " ON CONFLICT (window_start) DO UPDATE SET"
                                        + " window_end = EXCLUDED.window_end,"
                                        + " revenue_cents = EXCLUDED.revenue_cents,"
                                        + " event_count = EXCLUDED.event_count,"
                                        + " updated_at = now()",
                                (statement, window) -> {
                                    statement.setLong(1, window.windowStart);
                                    statement.setLong(2, window.windowEnd);
                                    statement.setLong(3, window.revenueCents);
                                    statement.setLong(4, window.eventCount);
                                },
                                execution,
                                connection))
                .name("revenue-windows-sink")
                .setParallelism(1);

        env.execute("ledgerline-stream");
    }
}
