package org.apache.flink.connector.clickhouse.internal;

import org.apache.flink.connector.clickhouse.internal.connection.ClickHouseConnectionProvider;
import org.apache.flink.connector.clickhouse.internal.executor.ClickHouseExecutor;
import org.apache.flink.connector.clickhouse.internal.options.ClickHouseDmlOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.sql.SQLException;

/** Output data to ClickHouse local table. */
public class ClickHouseBatchOutputFormat extends AbstractClickHouseOutputFormat {

    private static final long serialVersionUID = 1L;

    private final ClickHouseConnectionProvider connectionProvider;

    private final String[] fieldNames;

    private final String[] keyFields;

    private final String[] partitionFields;

    private final LogicalType[] fieldTypes;

    private final ClickHouseDmlOptions options;

    private transient ClickHouseExecutor executor;

    private transient int batchCount = 0;

    protected ClickHouseBatchOutputFormat(
            @Nonnull ClickHouseConnectionProvider connectionProvider,
            @Nonnull String[] fieldNames,
            @Nonnull String[] keyFields,
            @Nonnull String[] partitionFields,
            @Nonnull LogicalType[] fieldTypes,
            @Nonnull ClickHouseDmlOptions options) {
        this.connectionProvider = Preconditions.checkNotNull(connectionProvider);
        this.fieldNames = Preconditions.checkNotNull(fieldNames);
        this.keyFields = Preconditions.checkNotNull(keyFields);
        this.partitionFields = Preconditions.checkNotNull(partitionFields);
        this.fieldTypes = Preconditions.checkNotNull(fieldTypes);
        this.options = Preconditions.checkNotNull(options);
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        try {
            // TODO Distributed tables don't support update and delete statements.
            executor =
                    ClickHouseExecutor.createClickHouseExecutor(
                            options.getTableName(),
                            options.getDatabaseName(),
                            null,
                            fieldNames,
                            keyFields,
                            partitionFields,
                            fieldTypes,
                            options);
            executor.prepareStatement(connectionProvider);
            executor.setRuntimeContext(getRuntimeContext());

            long flushIntervalMillis = options.getFlushInterval().toMillis();
            scheduledFlush(flushIntervalMillis, "clickhouse-batch-output-format");
        } catch (Exception exception) {
            throw new IOException("Unable to establish connection with ClickHouse.", exception);
        }
    }

    @Override
    public synchronized void writeRecord(RowData record) throws IOException {
        checkFlushException();

        try {
            executor.addToBatch(record);
            batchCount++;
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
            checkBeforeFlush(executor);
            batchCount = 0;
        }
    }

    @Override
    public synchronized void closeOutputFormat() {
        executor.closeStatement();
        connectionProvider.closeConnections();
    }
}
