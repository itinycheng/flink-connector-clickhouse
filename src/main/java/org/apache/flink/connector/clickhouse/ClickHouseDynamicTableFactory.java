//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.flink.connector.clickhouse;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.clickhouse.config.ClickHouseConfig;
import org.apache.flink.connector.clickhouse.internal.options.ClickHouseOptions;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.FactoryUtil.TableFactoryHelper;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.connector.clickhouse.internal.partitioner.ClickHousePartitioner.BALANCED;
import static org.apache.flink.connector.clickhouse.internal.partitioner.ClickHousePartitioner.HASH;
import static org.apache.flink.connector.clickhouse.internal.partitioner.ClickHousePartitioner.SHUFFLE;

/** A {@link DynamicTableSinkFactory} for discovering {@link ClickHouseDynamicTableSink}. */
public class ClickHouseDynamicTableFactory implements DynamicTableSinkFactory {

    public static final String IDENTIFIER = "clickhouse";

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

    public static final ConfigOption<String> TABLE_NAME =
            ConfigOptions.key(ClickHouseConfig.TABLE_NAME)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The ClickHouse table name.");

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

    public ClickHouseDynamicTableFactory() {}

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig config = helper.getOptions();
        helper.validate();
        this.validateConfigOptions(config);
        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        return new ClickHouseDynamicTableSink(getOptions(config), physicalSchema);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(URL);
        requiredOptions.add(TABLE_NAME);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(USERNAME);
        optionalOptions.add(PASSWORD);
        optionalOptions.add(DATABASE_NAME);
        optionalOptions.add(SINK_BATCH_SIZE);
        optionalOptions.add(SINK_FLUSH_INTERVAL);
        optionalOptions.add(SINK_MAX_RETRIES);
        optionalOptions.add(SINK_WRITE_LOCAL);
        optionalOptions.add(SINK_PARTITION_STRATEGY);
        optionalOptions.add(SINK_PARTITION_KEY);
        optionalOptions.add(SINK_IGNORE_DELETE);
        return optionalOptions;
    }

    private void validateConfigOptions(ReadableConfig config) {
        String partitionStrategy = config.get(SINK_PARTITION_STRATEGY);
        if (!Arrays.asList(HASH, BALANCED, SHUFFLE).contains(partitionStrategy)) {
            throw new IllegalArgumentException(
                    String.format("Unknown sink.partition-strategy `%s`", partitionStrategy));
        } else if (HASH.equals(partitionStrategy)
                && !config.getOptional(SINK_PARTITION_KEY).isPresent()) {
            throw new IllegalArgumentException(
                    "A partition key must be provided for hash partition strategy");
        } else if (config.getOptional(USERNAME).isPresent()
                ^ config.getOptional(PASSWORD).isPresent()) {
            throw new IllegalArgumentException(
                    "Either all or none of username and password should be provided");
        }
    }

    private ClickHouseOptions getOptions(ReadableConfig config) {
        return new ClickHouseOptions.Builder()
                .withUrl(config.get(URL))
                .withUsername(config.get(USERNAME))
                .withPassword(config.get(PASSWORD))
                .withDatabaseName(config.get(DATABASE_NAME))
                .withTableName(config.get(TABLE_NAME))
                .withBatchSize(config.get(SINK_BATCH_SIZE))
                .withFlushInterval(config.get(SINK_FLUSH_INTERVAL))
                .withMaxRetries(config.get(SINK_MAX_RETRIES))
                .withWriteLocal(config.get(SINK_WRITE_LOCAL))
                .withPartitionStrategy(config.get(SINK_PARTITION_STRATEGY))
                .withPartitionKey(config.get(SINK_PARTITION_KEY))
                .withIgnoreDelete(config.get(SINK_IGNORE_DELETE))
                .build();
    }
}
