//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package com.tiny.flink.connector.clickhouse.internal;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.RowData.FieldGetter;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import com.tiny.flink.connector.clickhouse.internal.connection.ClickHouseConnectionProvider;
import com.tiny.flink.connector.clickhouse.internal.converter.ClickHouseRowConverter;
import com.tiny.flink.connector.clickhouse.internal.executor.ClickHouseBatchExecutor;
import com.tiny.flink.connector.clickhouse.internal.executor.ClickHouseExecutor;
import com.tiny.flink.connector.clickhouse.internal.options.ClickHouseOptions;
import com.tiny.flink.connector.clickhouse.internal.partitioner.ClickHousePartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Flushable;
import java.util.Arrays;
import java.util.List;

import static com.tiny.flink.connector.clickhouse.internal.partitioner.ClickHousePartitioner.BALANCED;
import static com.tiny.flink.connector.clickhouse.internal.partitioner.ClickHousePartitioner.HASH;
import static com.tiny.flink.connector.clickhouse.internal.partitioner.ClickHousePartitioner.SHUFFLE;

/** Abstract class of ClickHouse output format. */
public abstract class AbstractClickHouseOutputFormat extends RichOutputFormat<RowData>
        implements Flushable {

    private static final long serialVersionUID = 1L;

    public AbstractClickHouseOutputFormat() {}

    @Override
    public void configure(Configuration parameters) {}

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
            Preconditions.checkNotNull(this.options);
            Preconditions.checkNotNull(this.fieldNames);
            Preconditions.checkNotNull(this.fieldDataTypes);
            LogicalType[] logicalTypes =
                    Arrays.stream(this.fieldDataTypes)
                            .map(DataType::getLogicalType)
                            .toArray(LogicalType[]::new);
            ClickHouseRowConverter converter = new ClickHouseRowConverter(RowType.of(logicalTypes));
            if (this.primaryKey != null) {
                LOG.warn("If primary key is specified, connector will be in UPSERT mode.");
                LOG.warn("You will have significant performance loss.");
            }
            return this.options.getWriteLocal()
                    ? this.createShardOutputFormat(logicalTypes, converter)
                    : this.createBatchOutputFormat(converter);
        }

        private ClickHouseBatchOutputFormat createBatchOutputFormat(
                ClickHouseRowConverter converter) {
            ClickHouseExecutor executor;
            if (this.primaryKey != null && !this.options.getIgnoreDelete()) {
                executor =
                        ClickHouseExecutor.createUpsertExecutor(
                                this.options.getTableName(),
                                this.fieldNames,
                                this.listToStringArray(this.primaryKey.getColumns()),
                                converter,
                                this.options);
            } else {
                String sql =
                        ClickHouseStatementFactory.getInsertIntoStatement(
                                this.options.getTableName(), this.fieldNames);
                executor =
                        new ClickHouseBatchExecutor(
                                sql,
                                converter,
                                this.options.getFlushInterval(),
                                this.options.getBatchSize(),
                                this.options.getMaxRetries(),
                                this.rowDataTypeInformation);
            }

            return new ClickHouseBatchOutputFormat(
                    new ClickHouseConnectionProvider(this.options), executor, this.options);
        }

        private ClickHouseShardOutputFormat createShardOutputFormat(
                LogicalType[] logicalTypes, ClickHouseRowConverter converter) {
            String partitionStrategy = this.options.getPartitionStrategy();
            ClickHousePartitioner partitioner;
            switch (partitionStrategy) {
                case BALANCED:
                    partitioner = ClickHousePartitioner.createBalanced();
                    break;
                case SHUFFLE:
                    partitioner = ClickHousePartitioner.createShuffle();
                    break;
                case HASH:
                    int index =
                            Arrays.asList(this.fieldNames).indexOf(this.options.getPartitionKey());
                    if (index == -1) {
                        throw new IllegalArgumentException(
                                "Partition key `"
                                        + this.options.getPartitionKey()
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
            if (this.primaryKey != null && !this.options.getIgnoreDelete()) {
                keyFields = this.listToStringArray(this.primaryKey.getColumns());
            }
            return new ClickHouseShardOutputFormat(
                    new ClickHouseConnectionProvider(this.options),
                    this.fieldNames,
                    keyFields,
                    converter,
                    partitioner,
                    this.options);
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
