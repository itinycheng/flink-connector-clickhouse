//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.flink.connector.clickhouse.internal;

import org.apache.flink.connector.clickhouse.internal.executor.ClickHouseExecutor;
import org.apache.flink.connector.clickhouse.internal.partitioner.ClickHousePartitioner;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import org.apache.flink.connector.clickhouse.internal.connection.ClickHouseConnectionProvider;
import org.apache.flink.connector.clickhouse.internal.converter.ClickHouseRowConverter;
import org.apache.flink.connector.clickhouse.internal.executor.ClickHouseBatchExecutor;
import org.apache.flink.connector.clickhouse.internal.options.ClickHouseOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.ClickHouseConnection;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** The shard output format of distributed table. */
public class ClickHouseShardOutputFormat extends AbstractClickHouseOutputFormat {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseShardOutputFormat.class);

    private static final Pattern PATTERN =
            Pattern.compile(
                    "Distributed\\((?<cluster>[a-zA-Z_][0-9a-zA-Z_]*),\\s*(?<database>[a-zA-Z_][0-9a-zA-Z_]*),\\s*(?<table>[a-zA-Z_][0-9a-zA-Z_]*)");

    private final ClickHouseConnectionProvider connectionProvider;

    private final String[] fieldNames;

    private final ClickHouseRowConverter converter;

    private final ClickHousePartitioner partitioner;

    private final ClickHouseOptions options;

    private final List<ClickHouseExecutor> shardExecutors;

    private final boolean ignoreDelete;

    private final String[] keyFields;

    private transient String remoteTable;

    private transient int[] batchCounts;

    private transient List<ClickHouseConnection> shardConnections;

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
            establishShardConnections();
            initializeExecutors();

            long flushIntervalMillis = options.getFlushInterval().toMillis();
            if (flushIntervalMillis > 0) {
                scheduledFlush(flushIntervalMillis, "clickhouse-shard-output-format");
            }
        } catch (Exception exception) {
            throw new IOException("Unable to establish connection to ClickHouse", exception);
        }
    }

    private void establishShardConnections() throws IOException {
        try {
            String engine =
                    connectionProvider.queryTableEngine(
                            options.getDatabaseName(), options.getTableName());
            Matcher matcher = PATTERN.matcher(engine);
            if (matcher.find()) {
                String remoteCluster = matcher.group("cluster");
                String remoteDatabase = matcher.group("database");
                remoteTable = matcher.group("table");
                shardConnections =
                        connectionProvider.getShardConnections(remoteCluster, remoteDatabase);
                batchCounts = new int[shardConnections.size()];
            } else {
                throw new IOException(
                        "table `"
                                + options.getDatabaseName()
                                + "`.`"
                                + options.getTableName()
                                + "` is not a Distributed table");
            }
        } catch (SQLException exception) {
            throw new IOException("Establish shard connections failed.", exception);
        }
    }

    private void initializeExecutors() throws SQLException {
        String sql = ClickHouseStatementFactory.getInsertIntoStatement(remoteTable, fieldNames);
        for (ClickHouseConnection shardConnection : shardConnections) {
            ClickHouseExecutor executor;
            if (keyFields.length > 0) {
                // TODO why use upsert mode
                executor =
                        ClickHouseExecutor.createUpsertExecutor(
                                remoteTable, fieldNames, keyFields, converter, options);
            } else {
                executor = new ClickHouseBatchExecutor(sql, converter);
            }
            executor.prepareStatement(shardConnection);
            shardExecutors.add(executor);
        }
    }

    @Override
    public void writeRecord(RowData record) throws IOException {
        checkFlushException();

        switch (record.getRowKind()) {
            case INSERT:
                writeRecordToOneExecutor(record);
                break;
            case UPDATE_AFTER:
                if (ignoreDelete) {
                    writeRecordToOneExecutor(record);
                } else {
                    // TODO Why write record to all executors
                    writeRecordToAllExecutors(record);
                }
                break;
            case DELETE:
                // TODO Is delete statement exists, when was it generated.
                if (!ignoreDelete) {
                    this.writeRecordToAllExecutors(record);
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

    private void writeRecordToAllExecutors(RowData record) throws IOException {
        try {
            for (int i = 0; i < shardExecutors.size(); ++i) {
                shardExecutors.get(i).addToBatch(record);
                batchCounts[i]++;
                if (batchCounts[i] >= options.getBatchSize()) {
                    flush(i);
                }
            }
        } catch (Exception exception) {
            throw new IOException("Writing record to all executor failed.", exception);
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
            attemptFlush(shardExecutors.get(index), options.getMaxRetries());
            batchCounts[index] = 0;
        }
    }

    @Override
    public void closeOutputFormat() {
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
