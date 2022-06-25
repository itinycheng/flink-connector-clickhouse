package org.apache.flink.connector.clickhouse.config;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.factories.FactoryUtil;

import java.time.Duration;

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

    @Deprecated
    public static final ConfigOption<Boolean> SINK_WRITE_LOCAL =
            ConfigOptions.key(ClickHouseConfig.SINK_WRITE_LOCAL)
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Directly write to local tables in case of distributed table.");

    public static final ConfigOption<String> SINK_PARTITION_STRATEGY =
            ConfigOptions.key(ClickHouseConfig.SINK_PARTITION_STRATEGY)
                    .stringType()
                    .defaultValue("balanced")
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
}
