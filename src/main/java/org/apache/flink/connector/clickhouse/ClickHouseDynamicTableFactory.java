//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.flink.connector.clickhouse;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.clickhouse.internal.options.ClickHouseOptions;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.FactoryUtil.TableFactoryHelper;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.connector.clickhouse.config.ClickHouseConfig.IDENTIFIER;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.CATALOG_IGNORE_PRIMARY_KEY;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.DATABASE_NAME;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.PASSWORD;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SINK_BATCH_SIZE;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SINK_FLUSH_INTERVAL;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SINK_IGNORE_DELETE;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SINK_MAX_RETRIES;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SINK_PARTITION_KEY;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SINK_PARTITION_STRATEGY;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SINK_WRITE_LOCAL;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.TABLE_NAME;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.URL;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.USERNAME;
import static org.apache.flink.connector.clickhouse.internal.partitioner.ClickHousePartitioner.BALANCED;
import static org.apache.flink.connector.clickhouse.internal.partitioner.ClickHousePartitioner.HASH;
import static org.apache.flink.connector.clickhouse.internal.partitioner.ClickHousePartitioner.SHUFFLE;

/** A {@link DynamicTableSinkFactory} for discovering {@link ClickHouseDynamicTableSink}. */
public class ClickHouseDynamicTableFactory implements DynamicTableSinkFactory {

    public ClickHouseDynamicTableFactory() {}

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig config = helper.getOptions();
        helper.validate();
        this.validateConfigOptions(config);
        return new ClickHouseDynamicTableSink(
                getOptions(config),
                context.getCatalogTable(),
                context.getCatalogTable().getResolvedSchema());
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
        optionalOptions.add(CATALOG_IGNORE_PRIMARY_KEY);
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
