//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.flink.connector.clickhouse.internal;

import org.apache.flink.connector.clickhouse.internal.connection.ClickHouseConnectionProvider;
import org.apache.flink.connector.clickhouse.internal.executor.ClickHouseExecutor;
import org.apache.flink.connector.clickhouse.internal.options.ClickHouseOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.sql.SQLException;

/** Output data to ClickHouse local table. */
public class ClickHouseBatchOutputFormat extends AbstractClickHouseOutputFormat {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseBatchOutputFormat.class);

    private final ClickHouseConnectionProvider connectionProvider;

    private final ClickHouseExecutor executor;

    private final ClickHouseOptions options;

    private transient int batchCount = 0;

    protected ClickHouseBatchOutputFormat(
            @Nonnull ClickHouseConnectionProvider connectionProvider,
            @Nonnull ClickHouseExecutor executor,
            @Nonnull ClickHouseOptions options) {
        this.connectionProvider = Preconditions.checkNotNull(connectionProvider);
        this.executor = Preconditions.checkNotNull(executor);
        this.options = Preconditions.checkNotNull(options);
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        try {
            executor.prepareStatement(connectionProvider);
            executor.setRuntimeContext(getRuntimeContext());

            long flushIntervalMillis = options.getFlushInterval().toMillis();
            if (flushIntervalMillis > 0) {
                scheduledFlush(flushIntervalMillis, "clickhouse-batch-output-format");
            }
        } catch (Exception exception) {
            throw new IOException("Unable to establish connection with ClickHouse.", exception);
        }
    }

    @Override
    public void writeRecord(RowData record) throws IOException {
        checkFlushException();

        try {
            executor.addToBatch(record);
            ++batchCount;
            if (batchCount >= options.getBatchSize()) {
                flush();
            }
        } catch (SQLException exception) {
            throw new IOException("Writing record to ClickHouse statement failed.", exception);
        }
    }

    @Override
    public synchronized void flush() throws IOException {
        if (batchCount > 0) {
            attemptFlush(executor, options.getMaxRetries());
            batchCount = 0;
        }
    }

    @Override
    public void closeOutputFormat() {
        try {
            executor.closeStatement();
            connectionProvider.closeConnections();
        } catch (SQLException exception) {
            LOG.warn("ClickHouse connection could not be closed.", exception);
        }
    }
}
