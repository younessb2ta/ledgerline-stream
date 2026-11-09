package com.ledgerline.stream;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Idempotent balance sink plus a checkpointed snapshot of the projection.
 *
 * <p>Flink delivers at least once across a failover. The write is
 * {@code INSERT ... ON CONFLICT (customer_id) DO UPDATE}, and because
 * {@link BalanceFunction} emits the absolute running balance (not an
 * increment), replaying the same event writes the same number.
 *
 * <p>{@code notifyCheckpointComplete} copies the durable projection into
 * {@code balance_snapshots}. That table is the evidence that this operator saw
 * checkpoint N, and it lets the reconcile script compare the state in RocksDB
 * with what is actually in Postgres. The snapshot is read back from the
 * database on purpose: a map held only in heap would be empty after a restore
 * and would write an incomplete snapshot on the first checkpoint.
 */
public class BalanceSink extends RichSinkFunction<BalanceUpdate> implements CheckpointListener {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(BalanceSink.class);

    private final String jdbcUrl;
    private final String user;
    private final String password;

    private transient Connection connection;
    private transient PreparedStatement upsert;

    public BalanceSink(String jdbcUrl, String user, String password) {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("org.postgresql.Driver");
        connection = DriverManager.getConnection(jdbcUrl, user, password);
        connection.setAutoCommit(true);
        upsert =
                connection.prepareStatement(
                        "INSERT INTO customer_balances"
                                + " (customer_id, balance_cents, last_event_ts, updated_at)"
                                + " VALUES (?, ?, to_timestamp(? / 1000.0), now())"
                                + " ON CONFLICT (customer_id) DO UPDATE SET"
                                + " balance_cents = EXCLUDED.balance_cents,"
                                + " last_event_ts = EXCLUDED.last_event_ts,"
                                + " updated_at = now()"
                                // A replayed older update must not move the balance backwards.
                                + " WHERE EXCLUDED.last_event_ts >= customer_balances.last_event_ts");
    }

    @Override
    public void invoke(BalanceUpdate update, Context context) throws Exception {
        upsert.setString(1, update.customerId);
        upsert.setLong(2, update.balanceCents);
        upsert.setLong(3, update.eventTs);
        upsert.executeUpdate();
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        try (Statement statement = connection.createStatement();
                ResultSet rows =
                        statement.executeQuery(
                                "SELECT customer_id, balance_cents FROM customer_balances")) {
            try (PreparedStatement insert =
                    connection.prepareStatement(
                            "INSERT INTO balance_snapshots"
                                    + " (checkpoint_id, customer_id, balance_cents)"
                                    + " VALUES (?, ?, ?)"
                                    + " ON CONFLICT (checkpoint_id, customer_id) DO NOTHING")) {
                int pending = 0;
                while (rows.next()) {
                    insert.setLong(1, checkpointId);
                    insert.setString(2, rows.getString(1));
                    insert.setLong(3, rows.getLong(2));
                    insert.addBatch();
                    if (++pending % 500 == 0) {
                        insert.executeBatch();
                    }
                }
                insert.executeBatch();
            }
            LOG.info("wrote balance snapshot for checkpoint {}", checkpointId);
        } catch (SQLException exception) {
            // Bookkeeping must never fail the job; the balance write already happened.
            LOG.warn("could not write balance snapshot for checkpoint {}", checkpointId, exception);
        }
    }

    @Override
    public void close() throws Exception {
        if (upsert != null) {
            upsert.close();
        }
        if (connection != null) {
            connection.close();
        }
    }
}
