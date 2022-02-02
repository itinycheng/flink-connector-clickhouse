package org.apache.flink.connector.clickhouse.internal;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.clickhouse.internal.connection.ClickHouseConnectionProvider;
import org.apache.flink.connector.clickhouse.internal.executor.ClickHouseExecutor;
import org.apache.flink.connector.clickhouse.internal.options.ClickHouseDmlOptions;
import org.apache.flink.connector.clickhouse.internal.partitioner.ClickHousePartitioner;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.RowData.FieldGetter;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Flushable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/** Abstract class of ClickHouse output format. */
public abstract class AbstractClickHouseOutputFormat extends RichOutputFormat<RowData>
        implements Flushable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(AbstractClickHouseOutputFormat.class);

    protected transient volatile boolean closed = false;

    protected transient ScheduledExecutorService scheduler;

    protected transient ScheduledFuture<?> scheduledFuture;

    protected transient volatile Exception flushException;

    public AbstractClickHouseOutputFormat() {}

    @Override
    public void configure(Configuration parameters) {}

    public void scheduledFlush(long intervalMillis, String executorName) {
        Preconditions.checkArgument(intervalMillis > 0, "flush interval must be greater than 0");
        scheduler = new ScheduledThreadPoolExecutor(1, new ExecutorThreadFactory(executorName));
        scheduledFuture =
                scheduler.scheduleWithFixedDelay(
                        () -> {
                            synchronized (this) {
                                if (!closed) {
                                    try {
                                        flush();
                                    } catch (Exception e) {
                                        flushException = e;
                                    }
                                }
                            }
                        },
                        intervalMillis,
                        intervalMillis,
                        TimeUnit.MILLISECONDS);
    }

    public void checkBeforeFlush(final ClickHouseExecutor executor) throws IOException {
        checkFlushException();
        try {
            executor.executeBatch();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public synchronized void close() {
        if (!closed) {
            closed = true;

            try {
                flush();
            } catch (Exception exception) {
                LOG.warn("Flushing records to ClickHouse failed.", exception);
            }

            if (scheduledFuture != null) {
                scheduledFuture.cancel(false);
                this.scheduler.shutdown();
            }

            closeOutputFormat();
            checkFlushException();
        }
    }

    protected void checkFlushException() {
        if (flushException != null) {
            throw new RuntimeException("Flush exception found.", flushException);
        }
    }

    protected abstract void closeOutputFormat();

    /** Builder for {@link ClickHouseBatchOutputFormat} and {@link ClickHouseShardOutputFormat}. */
    public static class Builder {

        private static final Logger LOG =
                LoggerFactory.getLogger(AbstractClickHouseOutputFormat.Builder.class);

        private DataType[] fieldDataTypes;

        private ClickHouseDmlOptions options;

        private String[] fieldNames;

        private UniqueConstraint primaryKey;

        private List<String> partitionKeys;

        public Builder() {}

        public AbstractClickHouseOutputFormat.Builder withOptions(ClickHouseDmlOptions options) {
            this.options = options;
            return this;
        }

        public AbstractClickHouseOutputFormat.Builder withFieldDataTypes(
                DataType[] fieldDataTypes) {
            this.fieldDataTypes = fieldDataTypes;
            return this;
        }

        public AbstractClickHouseOutputFormat.Builder withFieldNames(String[] fieldNames) {
            this.fieldNames = fieldNames;
            return this;
        }

        public AbstractClickHouseOutputFormat.Builder withPrimaryKey(UniqueConstraint primaryKey) {
            this.primaryKey = primaryKey;
            return this;
        }

        public AbstractClickHouseOutputFormat.Builder withPartitionKey(List<String> partitionKeys) {
            this.partitionKeys = partitionKeys;
            return this;
        }

        public AbstractClickHouseOutputFormat build() {
            Preconditions.checkNotNull(options);
            Preconditions.checkNotNull(fieldNames);
            Preconditions.checkNotNull(fieldDataTypes);
            LogicalType[] logicalTypes =
                    Arrays.stream(fieldDataTypes)
                            .map(DataType::getLogicalType)
                            .toArray(LogicalType[]::new);
            if (primaryKey != null) {
                LOG.warn("If primary key is specified, connector will be in UPSERT mode.");
                LOG.warn(
                        "The data will be updated / deleted by the primary key, you will have significant performance loss.");
            }
            return options.isUseLocal()
                    ? createShardOutputFormat(logicalTypes)
                    : createBatchOutputFormat(logicalTypes);
        }

        private ClickHouseBatchOutputFormat createBatchOutputFormat(LogicalType[] logicalTypes) {
            String[] keyFields = new String[0];
            if (primaryKey != null) {
                keyFields = listToStringArray(primaryKey.getColumns());
            }

            return new ClickHouseBatchOutputFormat(
                    new ClickHouseConnectionProvider(options),
                    fieldNames,
                    keyFields,
                    listToStringArray(partitionKeys),
                    logicalTypes,
                    options);
        }

        private ClickHouseShardOutputFormat createShardOutputFormat(LogicalType[] logicalTypes) {
            String partitionStrategy = options.getPartitionStrategy();
            ClickHousePartitioner partitioner;
            switch (partitionStrategy) {
                case ClickHousePartitioner.BALANCED:
                    partitioner = ClickHousePartitioner.createBalanced();
                    break;
                case ClickHousePartitioner.SHUFFLE:
                    partitioner = ClickHousePartitioner.createShuffle();
                    break;
                case ClickHousePartitioner.HASH:
                    int index = Arrays.asList(fieldNames).indexOf(options.getPartitionKey());
                    if (index == -1) {
                        throw new IllegalArgumentException(
                                String.format(
                                        "Partition key `%s` not found in table schema",
                                        options.getPartitionKey()));
                    }
                    FieldGetter getter = RowData.createFieldGetter(logicalTypes[index], index);
                    partitioner = ClickHousePartitioner.createHash(getter);
                    break;
                default:
                    throw new IllegalArgumentException(
                            String.format(
                                    "Unknown sink.partition-strategy `%s`", partitionStrategy));
            }

            String[] keyFields = new String[0];
            if (primaryKey != null) {
                keyFields = listToStringArray(primaryKey.getColumns());
            }

            return new ClickHouseShardOutputFormat(
                    new ClickHouseConnectionProvider(options),
                    fieldNames,
                    keyFields,
                    listToStringArray(partitionKeys),
                    logicalTypes,
                    partitioner,
                    options);
        }

        private String[] listToStringArray(List<String> list) {
            if (list == null) {
                return new String[0];
            } else {
                return list.toArray(new String[0]);
            }
        }
    }
}
