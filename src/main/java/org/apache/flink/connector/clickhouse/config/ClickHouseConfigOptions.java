package org.apache.flink.connector.clickhouse.config;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.connector.clickhouse.internal.partitioner.BalancedPartitioner;
import org.apache.flink.connector.clickhouse.internal.partitioner.ClickHousePartitioner;
import org.apache.flink.connector.clickhouse.internal.partitioner.HashPartitioner;
import org.apache.flink.connector.clickhouse.internal.partitioner.ShufflePartitioner;
import org.apache.flink.connector.clickhouse.internal.partitioner.ValuePartitioner;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;

import java.io.Serializable;
import java.time.Duration;
import java.util.function.Function;

/** clickhouse config options. */
public class ClickHouseConfigOptions {

    public static final ConfigOption<String> URL =
            ConfigOptions.key(ClickHouseConfig.URL)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The ClickHouse url in format `clickhouse://<host>:<port>`.");

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key(ClickHouseConfig.USERNAME)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The ClickHouse username.");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key(ClickHouseConfig.PASSWORD)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The ClickHouse password.");

    public static final ConfigOption<String> DATABASE_NAME =
            ConfigOptions.key(ClickHouseConfig.DATABASE_NAME)
                    .stringType()
                    .defaultValue("default")
                    .withDescription("The ClickHouse database name. Default to `default`.");

    public static final ConfigOption<String> DEFAULT_DATABASE =
            ConfigOptions.key(CommonCatalogOptions.DEFAULT_DATABASE_KEY)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The ClickHouse default database name.");

    public static final ConfigOption<String> TABLE_NAME =
            ConfigOptions.key(ClickHouseConfig.TABLE_NAME)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The ClickHouse table name.");

    public static final ConfigOption<Boolean> USE_LOCAL =
            ConfigOptions.key(ClickHouseConfig.USE_LOCAL)
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Directly read/write local tables in case of distributed table engine.");

    public static final ConfigOption<Integer> SINK_BATCH_SIZE =
            ConfigOptions.key(ClickHouseConfig.SINK_BATCH_SIZE)
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "The max flush size, over this number of records, will flush data. The default value is 1000.");

    public static final ConfigOption<Duration> SINK_FLUSH_INTERVAL =
            ConfigOptions.key(ClickHouseConfig.SINK_FLUSH_INTERVAL)
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1L))
                    .withDescription(
                            "The flush interval mills, over this time, asynchronous threads will flush data. The default value is 1s.");

    public static final ConfigOption<Integer> SINK_MAX_RETRIES =
            ConfigOptions.key(ClickHouseConfig.SINK_MAX_RETRIES)
                    .intType()
                    .defaultValue(3)
                    .withDescription("The max retry times if writing records to database failed.");

    public static final ConfigOption<SinkUpdateStrategy> SINK_UPDATE_STRATEGY =
            ConfigOptions.key(ClickHouseConfig.SINK_UPDATE_STRATEGY)
                    .enumType(SinkUpdateStrategy.class)
                    .defaultValue(SinkUpdateStrategy.UPDATE)
                    .withDescription(
                            "Convert a record of type UPDATE_AFTER to update/insert statement or just discard it, available: update, insert, discard."
                                    + " Additional: `table.exec.sink.upsert-materialize`, `org.apache.flink.table.runtime.operators.sink.SinkUpsertMaterializer`");

    public static final ConfigOption<SinkPartitionStrategy> SINK_PARTITION_STRATEGY =
            ConfigOptions.key(ClickHouseConfig.SINK_PARTITION_STRATEGY)
                    .enumType(SinkPartitionStrategy.class)
                    .defaultValue(SinkPartitionStrategy.BALANCED)
                    .withDescription("Partition strategy, available: balanced, hash, shuffle.");

    public static final ConfigOption<String> SINK_PARTITION_KEY =
            ConfigOptions.key(ClickHouseConfig.SINK_PARTITION_KEY)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Partition key used for hash strategy.");

    public static final ConfigOption<Boolean> SINK_IGNORE_DELETE =
            ConfigOptions.key(ClickHouseConfig.SINK_IGNORE_DELETE)
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to treat update statements as insert statements and ignore deletes. defaults to true.");

    public static final ConfigOption<Integer> SINK_PARALLELISM = FactoryUtil.SINK_PARALLELISM;

    public static final ConfigOption<Boolean> CATALOG_IGNORE_PRIMARY_KEY =
            ConfigOptions.key(ClickHouseConfig.CATALOG_IGNORE_PRIMARY_KEY)
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to ignore primary keys when using ClickHouseCatalog to create table. defaults to true.");

    public static final ConfigOption<String> SCAN_PARTITION_COLUMN =
            ConfigOptions.key(ClickHouseConfig.SCAN_PARTITION_COLUMN)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The column name used for partitioning the input.");

    public static final ConfigOption<Integer> SCAN_PARTITION_NUM =
            ConfigOptions.key(ClickHouseConfig.SCAN_PARTITION_NUM)
                    .intType()
                    .noDefaultValue()
                    .withDescription("The number of partitions.");

    public static final ConfigOption<Long> SCAN_PARTITION_LOWER_BOUND =
            ConfigOptions.key(ClickHouseConfig.SCAN_PARTITION_LOWER_BOUND)
                    .longType()
                    .noDefaultValue()
                    .withDescription("The smallest value of the first partition.");

    public static final ConfigOption<Long> SCAN_PARTITION_UPPER_BOUND =
            ConfigOptions.key(ClickHouseConfig.SCAN_PARTITION_UPPER_BOUND)
                    .longType()
                    .noDefaultValue()
                    .withDescription("The largest value of the last partition.");

    /** Partition strategy for sink operator. */
    public enum SinkPartitionStrategy {
        BALANCED(
                "balanced",
                "Round robin.",
                false,
                (Function<RowData.FieldGetter, ClickHousePartitioner> & Serializable)
                        fieldGetter -> new BalancedPartitioner()),

        SHUFFLE(
                "shuffle",
                "Randomly choose a partitioner.",
                false,
                (Function<RowData.FieldGetter, ClickHousePartitioner> & Serializable)
                        fieldGetter -> new ShufflePartitioner()),

        HASH(
                "hash",
                "Generate hash value by calling `Objects.hashcode`.",
                true,
                (Function<RowData.FieldGetter, ClickHousePartitioner> & Serializable)
                        HashPartitioner::new),

        VALUE(
                "value",
                "Partition by partition key value, must be a number.",
                true,
                (Function<RowData.FieldGetter, ClickHousePartitioner> & Serializable)
                        ValuePartitioner::new);

        public final String value;

        public final String description;

        public final boolean partitionKeyNeeded;

        public final Function<RowData.FieldGetter, ClickHousePartitioner> provider;

        SinkPartitionStrategy(
                String value,
                String description,
                boolean partitionKeyNeeded,
                Function<RowData.FieldGetter, ClickHousePartitioner> provider) {
            this.value = value;
            this.description = description;
            this.partitionKeyNeeded = partitionKeyNeeded;
            this.provider = provider;
        }
    }

    /** Update conversion strategy for sink operator. */
    public enum SinkUpdateStrategy {
        UPDATE("update", "Convert UPDATE_AFTER records to update statement."),
        INSERT("insert", "Convert UPDATE_AFTER records to insert statement."),
        DISCARD("discard", "Discard UPDATE_AFTER records.");

        private final String value;
        private final String description;

        SinkUpdateStrategy(String value, String description) {
            this.value = value;
            this.description = description;
        }

        @Override
        public String toString() {
            return value;
        }

        public String getDescription() {
            return description;
        }
    }
}
