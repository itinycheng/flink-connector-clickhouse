package org.apache.flink.connector.clickhouse.internal;

import org.apache.flink.connector.clickhouse.internal.common.DistributedEngineFullSchema;
import org.apache.flink.connector.clickhouse.internal.connection.ClickHouseConnectionProvider;
import org.apache.flink.connector.clickhouse.internal.executor.ClickHouseExecutor;
import org.apache.flink.connector.clickhouse.internal.options.ClickHouseDmlOptions;
import org.apache.flink.connector.clickhouse.internal.partitioner.ClickHousePartitioner;
import org.apache.flink.connector.clickhouse.util.ClickHouseUtil;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Preconditions;

import ru.yandex.clickhouse.ClickHouseConnection;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The shard output format of distributed table.<br>
 * TODO: Use ClickHouse's sharding key to distribute data to different instances.
 */
public class ClickHouseShardOutputFormat extends AbstractClickHouseOutputFormat {

    private static final long serialVersionUID = 1L;

    private final ClickHouseConnectionProvider connectionProvider;

    private final String[] fieldNames;

    private final LogicalType[] logicalTypes;

    private final ClickHousePartitioner partitioner;

    private final ClickHouseDmlOptions options;

    private final List<ClickHouseExecutor> shardExecutors;

    private final String[] keyFields;

    private final String[] partitionFields;

    private transient int[] batchCounts;

    protected ClickHouseShardOutputFormat(
            @Nonnull ClickHouseConnectionProvider connectionProvider,
            @Nonnull String[] fieldNames,
            @Nonnull String[] keyFields,
            @Nonnull String[] partitionFields,
            @Nonnull LogicalType[] logicalTypes,
            @Nonnull ClickHousePartitioner partitioner,
            @Nonnull ClickHouseDmlOptions options) {
        this.connectionProvider = Preconditions.checkNotNull(connectionProvider);
        this.fieldNames = Preconditions.checkNotNull(fieldNames);
        this.keyFields = keyFields;
        this.partitionFields = partitionFields;
        this.logicalTypes = Preconditions.checkNotNull(logicalTypes);
        this.partitioner = Preconditions.checkNotNull(partitioner);
        this.options = Preconditions.checkNotNull(options);
        this.shardExecutors = new ArrayList<>();
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        try {
            // Get the local table of distributed table.
            DistributedEngineFullSchema shardTableSchema =
                    ClickHouseUtil.getAndParseDistributedEngineSchema(
                            connectionProvider.getOrCreateConnection(),
                            options.getDatabaseName(),
                            options.getTableName());
            if (shardTableSchema == null) {
                throw new RuntimeException(
                        String.format(
                                "table `%s`.`%s` is not a Distributed table",
                                options.getDatabaseName(), options.getTableName()));
            }

            List<ClickHouseConnection> shardConnections =
                    connectionProvider.createShardConnections(
                            shardTableSchema.getCluster(), shardTableSchema.getDatabase());
            for (ClickHouseConnection shardConnection : shardConnections) {
                ClickHouseExecutor executor =
                        ClickHouseExecutor.createClickHouseExecutor(
                                shardTableSchema.getTable(),
                                shardTableSchema.getDatabase(),
                                shardTableSchema.getCluster(),
                                fieldNames,
                                keyFields,
                                partitionFields,
                                logicalTypes,
                                options);
                executor.prepareStatement(shardConnection);
                shardExecutors.add(executor);
            }

            batchCounts = new int[shardConnections.size()];
            long flushIntervalMillis = options.getFlushInterval().toMillis();
            scheduledFlush(flushIntervalMillis, "clickhouse-shard-output-format");
        } catch (Exception exception) {
            throw new IOException("Unable to establish connection to ClickHouse", exception);
        }
    }

    /**
     * TODO: It's not appropriate to write records in this way, we should adapt it to ClickHouse's
     * data shard strategy.
     */
    @Override
    public synchronized void writeRecord(RowData record) throws IOException {
        checkFlushException();

        switch (record.getRowKind()) {
            case INSERT:
            case UPDATE_AFTER:
            case DELETE:
                writeRecordToOneExecutor(record);
                break;
            case UPDATE_BEFORE:
                break;
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Unknown row kind, the supported row kinds is: INSERT, UPDATE_BEFORE, UPDATE_AFTER, DELETE, but get: %s.",
                                record.getRowKind()));
        }
    }

    private void writeRecordToOneExecutor(RowData record) throws IOException {
        try {
            int selected = partitioner.select(record, shardExecutors.size());
            shardExecutors.get(selected).addToBatch(record);
            batchCounts[selected]++;
            if (batchCounts[selected] >= options.getBatchSize()) {
                flush(selected);
            }
        } catch (Exception exception) {
            throw new IOException("Writing record to one executor failed.", exception);
        }
    }

    @Override
    public synchronized void flush() throws IOException {
        for (int i = 0; i < shardExecutors.size(); i++) {
            flush(i);
        }
    }

    private synchronized void flush(int index) throws IOException {
        if (batchCounts[index] > 0) {
            checkBeforeFlush(shardExecutors.get(index));
            batchCounts[index] = 0;
        }
    }

    @Override
    public synchronized void closeOutputFormat() {
        for (ClickHouseExecutor shardExecutor : shardExecutors) {
            shardExecutor.closeStatement();
        }
        connectionProvider.closeConnections();
    }
}
