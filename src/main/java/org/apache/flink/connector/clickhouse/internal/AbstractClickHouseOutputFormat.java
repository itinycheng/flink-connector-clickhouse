//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.flink.connector.clickhouse.internal;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.clickhouse.internal.converter.ClickHouseRowConverter;
import org.apache.flink.connector.clickhouse.internal.executor.ClickHouseExecutor;
import org.apache.flink.connector.clickhouse.internal.partitioner.ClickHousePartitioner;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.RowData.FieldGetter;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import org.apache.flink.connector.clickhouse.internal.connection.ClickHouseConnectionProvider;
import org.apache.flink.connector.clickhouse.internal.executor.ClickHouseBatchExecutor;
import org.apache.flink.connector.clickhouse.internal.options.ClickHouseOptions;

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

    public void attemptFlush(final ClickHouseExecutor executor, final int retryAttempt)
            throws IOException {
        checkFlushException();

        for (int i = 0; i < retryAttempt; i++) {
            try {
                executor.executeBatch();
                return;
            } catch (Exception e) {
                LOG.error("ClickHouse executeBatch error, retry times = {}", i, e);
                try {
                    Thread.sleep(1000 * i);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new IOException(
                            "Unable to flush, interrupted while doing another attempt.", e);
                }
            }
        }

        throw new IOException(
                String.format(
                        "Attempt to Flush ClickHouse executor failed, exhausted retry times = %d",
                        retryAttempt));
    }

    @Override
    public synchronized void close() {
        if (!closed) {
            closed = true;

            try {
                flush();
            } catch (Exception exception) {
                LOG.warn("Writing records to ClickHouse failed.", exception);
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
            throw new RuntimeException("Writing records to ClickHouse failed.", flushException);
        }
    }

    protected abstract void closeOutputFormat();

    /** Builder for {@link ClickHouseBatchOutputFormat} and {@link ClickHouseShardOutputFormat}. */
    public static class Builder {

        private static final Logger LOG =
                LoggerFactory.getLogger(AbstractClickHouseOutputFormat.Builder.class);

        private DataType[] fieldDataTypes;

        private ClickHouseOptions options;

        private String[] fieldNames;

        private UniqueConstraint primaryKey;

        private TypeInformation<RowData> rowDataTypeInformation;

        public Builder() {}

        public AbstractClickHouseOutputFormat.Builder withOptions(ClickHouseOptions options) {
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

        public AbstractClickHouseOutputFormat.Builder withRowDataTypeInfo(
                TypeInformation<RowData> rowDataTypeInfo) {
            this.rowDataTypeInformation = rowDataTypeInfo;
            return this;
        }

        public AbstractClickHouseOutputFormat.Builder withPrimaryKey(UniqueConstraint primaryKey) {
            this.primaryKey = primaryKey;
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
            ClickHouseRowConverter converter = new ClickHouseRowConverter(RowType.of(logicalTypes));
            if (primaryKey != null) {
                LOG.warn("If primary key is specified, connector will be in UPSERT mode.");
                LOG.warn("You will have significant performance loss.");
            }
            return options.getWriteLocal()
                    ? createShardOutputFormat(logicalTypes, converter)
                    : createBatchOutputFormat(converter);
        }

        private ClickHouseBatchOutputFormat createBatchOutputFormat(
                ClickHouseRowConverter converter) {
            ClickHouseExecutor executor;
            if (primaryKey != null && !options.getIgnoreDelete()) {
                executor =
                        ClickHouseExecutor.createUpsertExecutor(
                                options.getTableName(),
                                fieldNames,
                                listToStringArray(primaryKey.getColumns()),
                                converter,
                                options);
            } else {
                String sql =
                        ClickHouseStatementFactory.getInsertIntoStatement(
                                options.getTableName(), fieldNames);
                executor = new ClickHouseBatchExecutor(sql, converter);
            }

            return new ClickHouseBatchOutputFormat(
                    new ClickHouseConnectionProvider(options), executor, options);
        }

        private ClickHouseShardOutputFormat createShardOutputFormat(
                LogicalType[] logicalTypes, ClickHouseRowConverter converter) {
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
                                "Partition key `"
                                        + options.getPartitionKey()
                                        + "` not found in table schema");
                    }
                    FieldGetter getter = RowData.createFieldGetter(logicalTypes[index], index);
                    partitioner = ClickHousePartitioner.createHash(getter);
                    break;
                default:
                    throw new IllegalArgumentException(
                            "Unknown sink.partition-strategy `" + partitionStrategy + "`");
            }

            String[] keyFields = new String[0];
            if (primaryKey != null && !options.getIgnoreDelete()) {
                keyFields = listToStringArray(primaryKey.getColumns());
            }
            return new ClickHouseShardOutputFormat(
                    new ClickHouseConnectionProvider(options),
                    fieldNames,
                    keyFields,
                    converter,
                    partitioner,
                    options);
        }

        private String[] listToStringArray(List<String> lists) {
            if (lists == null) {
                return new String[0];
            } else {
                return lists.toArray(new String[0]);
            }
        }
    }
}
