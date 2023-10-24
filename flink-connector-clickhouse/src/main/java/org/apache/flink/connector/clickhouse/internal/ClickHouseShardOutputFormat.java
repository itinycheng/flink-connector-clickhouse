package org.apache.flink.connector.clickhouse.internal;

import org.apache.flink.connector.clickhouse.internal.connection.ClickHouseConnectionProvider;
import org.apache.flink.connector.clickhouse.internal.executor.ClickHouseExecutor;
import org.apache.flink.connector.clickhouse.internal.options.ClickHouseDmlOptions;
import org.apache.flink.connector.clickhouse.internal.partitioner.ClickHousePartitioner;
import org.apache.flink.connector.clickhouse.internal.schema.ClusterSpec;
import org.apache.flink.connector.clickhouse.internal.schema.DistributedEngineFull;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Preconditions;

import com.clickhouse.jdbc.ClickHouseConnection;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/** The shard output format of distributed table. */
public class ClickHouseShardOutputFormat extends AbstractClickHouseOutputFormat {

    private static final long serialVersionUID = 1L;

    private final ClickHouseConnectionProvider connectionProvider;

    private final ClusterSpec clusterSpec;

    private final DistributedEngineFull shardTableSchema;

    private final String[] fieldNames;

    private final String[] keyFields;

    private final String[] partitionFields;

    private final LogicalType[] logicalTypes;

    private final ClickHousePartitioner partitioner;

    private final ClickHouseDmlOptions options;

    private final Map<Integer, ClickHouseExecutor> shardExecutorMap;

    private final Map<Integer, AtomicInteger> batchCountMap;

    protected ClickHouseShardOutputFormat(
            @Nonnull ClickHouseConnectionProvider connectionProvider,
            @Nonnull ClusterSpec clusterSpec,
            @Nonnull DistributedEngineFull shardTableSchema,
            @Nonnull String[] fieldNames,
            @Nonnull String[] keyFields,
            @Nonnull String[] partitionFields,
            @Nonnull LogicalType[] logicalTypes,
            @Nonnull ClickHousePartitioner partitioner,
            @Nonnull ClickHouseDmlOptions options) {
        this.connectionProvider = Preconditions.checkNotNull(connectionProvider);
        this.clusterSpec = Preconditions.checkNotNull(clusterSpec);
        this.shardTableSchema = Preconditions.checkNotNull(shardTableSchema);
        this.fieldNames = Preconditions.checkNotNull(fieldNames);
        this.keyFields = keyFields;
        this.partitionFields = partitionFields;
        this.logicalTypes = Preconditions.checkNotNull(logicalTypes);
        this.partitioner = Preconditions.checkNotNull(partitioner);
        this.options = Preconditions.checkNotNull(options);
        this.shardExecutorMap = new HashMap<>();
        this.batchCountMap = new HashMap<>();
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        try {
            Map<Integer, ClickHouseConnection> connectionMap =
                    connectionProvider.createShardConnections(
                            clusterSpec, shardTableSchema.getDatabase());
            for (Map.Entry<Integer, ClickHouseConnection> connectionEntry :
                    connectionMap.entrySet()) {
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
                executor.prepareStatement(connectionEntry.getValue());
                shardExecutorMap.put(connectionEntry.getKey(), executor);
            }

            long flushIntervalMillis = options.getFlushInterval().toMillis();
            scheduledFlush(flushIntervalMillis, "clickhouse-shard-output-format");
        } catch (Exception exception) {
            throw new IOException("Unable to establish connection to ClickHouse", exception);
        }
    }

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
            int shardNum = partitioner.select(record, clusterSpec);
            shardExecutorMap.get(shardNum).addToBatch(record);
            int batchCount =
                    batchCountMap
                            .computeIfAbsent(shardNum, integer -> new AtomicInteger(0))
                            .incrementAndGet();
            if (batchCount >= options.getBatchSize()) {
                flush(shardNum);
            }
        } catch (Exception exception) {
            throw new IOException("Writing record to one executor failed.", exception);
        }
    }

    @Override
    public synchronized void flush() throws IOException {
        for (Integer shardNum : shardExecutorMap.keySet()) {
            flush(shardNum);
        }
    }

    private synchronized void flush(int shardNum) throws IOException {
        AtomicInteger batchCount = batchCountMap.get(shardNum);
        if (batchCount != null && batchCount.intValue() > 0) {
            checkBeforeFlush(shardExecutorMap.get(shardNum));
            batchCount.set(0);
        }
    }

    @Override
    public synchronized void closeOutputFormat() {
        for (ClickHouseExecutor shardExecutor : shardExecutorMap.values()) {
            shardExecutor.closeStatement();
        }
        connectionProvider.closeConnections();
    }
}
