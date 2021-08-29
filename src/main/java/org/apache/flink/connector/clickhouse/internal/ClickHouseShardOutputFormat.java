//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.flink.connector.clickhouse.internal;

import org.apache.flink.connector.clickhouse.internal.connection.ClickHouseConnectionProvider;
import org.apache.flink.connector.clickhouse.internal.converter.ClickHouseRowConverter;
import org.apache.flink.connector.clickhouse.internal.executor.ClickHouseExecutor;
import org.apache.flink.connector.clickhouse.internal.options.ClickHouseOptions;
import org.apache.flink.connector.clickhouse.internal.partitioner.ClickHousePartitioner;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.ClickHouseConnection;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * The shard output format of distributed table.<br>
 * TODO: Use ClickHouse's sharding key to distribute data to different instances.
 */
public class ClickHouseShardOutputFormat extends AbstractClickHouseOutputFormat {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseShardOutputFormat.class);

    private final ClickHouseConnectionProvider connectionProvider;

    private final String[] fieldNames;

    private final ClickHouseRowConverter converter;

    private final ClickHousePartitioner partitioner;

    private final ClickHouseOptions options;

    private final List<ClickHouseExecutor> shardExecutors;

    private final boolean ignoreDelete;

    private final String[] keyFields;

    private transient int[] batchCounts;

    protected ClickHouseShardOutputFormat(
            @Nonnull ClickHouseConnectionProvider connectionProvider,
            @Nonnull String[] fieldNames,
            @Nonnull String[] keyFields,
            @Nonnull ClickHouseRowConverter converter,
            @Nonnull ClickHousePartitioner partitioner,
            @Nonnull ClickHouseOptions options) {
        this.connectionProvider = Preconditions.checkNotNull(connectionProvider);
        this.fieldNames = Preconditions.checkNotNull(fieldNames);
        this.converter = Preconditions.checkNotNull(converter);
        this.partitioner = Preconditions.checkNotNull(partitioner);
        this.options = Preconditions.checkNotNull(options);
        this.shardExecutors = new ArrayList<>();
        this.ignoreDelete = options.getIgnoreDelete();
        this.keyFields = keyFields;
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        try {
            List<ClickHouseConnection> shardConnections =
                    connectionProvider.getOrCreateShardConnections();
            String shardTable = connectionProvider.getShardTable();
            for (ClickHouseConnection shardConnection : shardConnections) {
                ClickHouseExecutor executor =
                        ClickHouseExecutor.createClickHouseExecutor(
                                shardTable, fieldNames, keyFields, converter, options);
                executor.prepareStatement(shardConnection);
                shardExecutors.add(executor);
            }
            batchCounts = new int[shardConnections.size()];
            long flushIntervalMillis = options.getFlushInterval().toMillis();
            if (flushIntervalMillis > 0) {
                scheduledFlush(flushIntervalMillis, "clickhouse-shard-output-format");
            }
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
                writeRecordToOneExecutor(record);
                break;
            case DELETE:
                if (!ignoreDelete) {
                    writeRecordToOneExecutor(record);
                }
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
        for (int i = 0; i < shardExecutors.size(); ++i) {
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
        try {
            for (ClickHouseExecutor shardExecutor : shardExecutors) {
                shardExecutor.closeStatement();
            }

            connectionProvider.closeConnections();
        } catch (SQLException exception) {
            LOG.warn("ClickHouse connection could not be closed.", exception);
        }
    }
}
