package org.apache.flink.connector.clickhouse;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SinkShardingStrategy;
import org.apache.flink.connector.clickhouse.internal.options.ClickHouseDmlOptions;
import org.apache.flink.connector.clickhouse.internal.options.ClickHouseReadOptions;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.lookup.LookupOptions;
import org.apache.flink.table.connector.source.lookup.cache.DefaultLookupCache;
import org.apache.flink.table.connector.source.lookup.cache.LookupCache;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.FactoryUtil.TableFactoryHelper;

import javax.annotation.Nullable;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.connector.clickhouse.config.ClickHouseConfig.IDENTIFIER;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfig.PROPERTIES_PREFIX;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.CATALOG_IGNORE_PRIMARY_KEY;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.DATABASE_NAME;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.PASSWORD;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SCAN_PARTITION_COLUMN;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SCAN_PARTITION_LOWER_BOUND;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SCAN_PARTITION_NUM;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SCAN_PARTITION_UPPER_BOUND;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SINK_BATCH_SIZE;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SINK_FLUSH_INTERVAL;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SINK_IGNORE_DELETE;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SINK_MAX_RETRIES;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SINK_PARALLELISM;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SINK_PARTITION_KEY;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SINK_PARTITION_STRATEGY;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SINK_SHARDING_USE_TABLE_DEF;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SINK_UPDATE_STRATEGY;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.TABLE_NAME;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.URL;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.USERNAME;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.USE_LOCAL;
import static org.apache.flink.connector.clickhouse.util.ClickHouseUtil.getClickHouseProperties;

/** A {@link DynamicTableSinkFactory} for discovering {@link ClickHouseDynamicTableSink}. */
public class ClickHouseDynamicTableFactory
        implements DynamicTableSinkFactory, DynamicTableSourceFactory {

    public ClickHouseDynamicTableFactory() {}

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig config = helper.getOptions();
        helper.validateExcept(PROPERTIES_PREFIX);
        validateConfigOptions(config);

        ResolvedCatalogTable catalogTable = context.getCatalogTable();
        String[] primaryKeys =
                catalogTable
                        .getResolvedSchema()
                        .getPrimaryKey()
                        .map(UniqueConstraint::getColumns)
                        .map(keys -> keys.toArray(new String[0]))
                        .orElse(new String[0]);
        Properties clickHouseProperties =
                getClickHouseProperties(context.getCatalogTable().getOptions());
        return new ClickHouseDynamicTableSink(
                getDmlOptions(config),
                clickHouseProperties,
                primaryKeys,
                catalogTable.getPartitionKeys().toArray(new String[0]),
                context.getPhysicalRowDataType());
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig config = helper.getOptions();
        helper.validateExcept(PROPERTIES_PREFIX);
        validateConfigOptions(config);

        Properties clickHouseProperties =
                getClickHouseProperties(context.getCatalogTable().getOptions());
        return new ClickHouseDynamicTableSource(
                getReadOptions(config),
                helper.getOptions().get(LookupOptions.MAX_RETRIES),
                getLookupCache(config),
                clickHouseProperties,
                context.getPhysicalRowDataType());
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
        optionalOptions.add(USE_LOCAL);
        optionalOptions.add(SINK_BATCH_SIZE);
        optionalOptions.add(SINK_FLUSH_INTERVAL);
        optionalOptions.add(SINK_MAX_RETRIES);
        optionalOptions.add(SINK_UPDATE_STRATEGY);
        optionalOptions.add(SINK_PARTITION_STRATEGY);
        optionalOptions.add(SINK_PARTITION_KEY);
        optionalOptions.add(SINK_SHARDING_USE_TABLE_DEF);
        optionalOptions.add(SINK_IGNORE_DELETE);
        optionalOptions.add(SINK_PARALLELISM);
        optionalOptions.add(CATALOG_IGNORE_PRIMARY_KEY);
        optionalOptions.add(SCAN_PARTITION_COLUMN);
        optionalOptions.add(SCAN_PARTITION_NUM);
        optionalOptions.add(SCAN_PARTITION_LOWER_BOUND);
        optionalOptions.add(SCAN_PARTITION_UPPER_BOUND);
        optionalOptions.add(LookupOptions.CACHE_TYPE);
        optionalOptions.add(LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_ACCESS);
        optionalOptions.add(LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_WRITE);
        optionalOptions.add(LookupOptions.PARTIAL_CACHE_MAX_ROWS);
        optionalOptions.add(LookupOptions.PARTIAL_CACHE_CACHE_MISSING_KEY);
        optionalOptions.add(LookupOptions.MAX_RETRIES);
        return optionalOptions;
    }

    private void validateConfigOptions(ReadableConfig config) {
        SinkShardingStrategy shardingStrategy = config.get(SINK_PARTITION_STRATEGY);
        if (!config.get(SINK_SHARDING_USE_TABLE_DEF)
                && shardingStrategy.shardingKeyNeeded
                && !config.getOptional(SINK_PARTITION_KEY).isPresent()) {
            throw new IllegalArgumentException(
                    "A sharding key must be provided for sharding strategy: "
                            + shardingStrategy.value);
        } else if (config.getOptional(USERNAME).isPresent()
                ^ config.getOptional(PASSWORD).isPresent()) {
            throw new IllegalArgumentException(
                    "Either all or none of username and password should be provided");
        } else if (!config.get(LookupOptions.CACHE_TYPE).equals(LookupOptions.LookupCacheType.NONE)
                && !config.get(LookupOptions.CACHE_TYPE)
                        .equals(LookupOptions.LookupCacheType.PARTIAL)) {
            throw new IllegalArgumentException(
                    String.format(
                            "The value of '%s' option should be 'NONE' or 'PARTIAL'(not support 'FULL' yet), but is %s.",
                            LookupOptions.CACHE_TYPE.key(), config.get(LookupOptions.CACHE_TYPE)));
        } else if (config.getOptional(SCAN_PARTITION_COLUMN).isPresent()
                ^ config.getOptional(SCAN_PARTITION_NUM).isPresent()
                ^ config.getOptional(SCAN_PARTITION_LOWER_BOUND).isPresent()
                ^ config.getOptional(SCAN_PARTITION_UPPER_BOUND).isPresent()) {
            throw new IllegalArgumentException(
                    "Either all or none of partition configs should be provided");
        } else if (config.get(LookupOptions.MAX_RETRIES) < 0) {
            throw new IllegalArgumentException(
                    String.format(
                            "The value of '%s' option shouldn't be negative, but is %s.",
                            LookupOptions.MAX_RETRIES.key(),
                            config.get(LookupOptions.MAX_RETRIES)));
        }
    }

    private ClickHouseDmlOptions getDmlOptions(ReadableConfig config) {
        return new ClickHouseDmlOptions.Builder()
                .withUrl(config.get(URL))
                .withUsername(config.get(USERNAME))
                .withPassword(config.get(PASSWORD))
                .withDatabaseName(config.get(DATABASE_NAME))
                .withTableName(config.get(TABLE_NAME))
                .withBatchSize(config.get(SINK_BATCH_SIZE))
                .withFlushInterval(config.get(SINK_FLUSH_INTERVAL))
                .withMaxRetries(config.get(SINK_MAX_RETRIES))
                .withUseLocal(config.get(USE_LOCAL))
                .withUpdateStrategy(config.get(SINK_UPDATE_STRATEGY))
                .withShardingStrategy(config.get(SINK_PARTITION_STRATEGY))
                .withShardingKey(config.get(SINK_PARTITION_KEY))
                .withUseTableDef(config.get(SINK_SHARDING_USE_TABLE_DEF))
                .withIgnoreDelete(config.get(SINK_IGNORE_DELETE))
                .withParallelism(config.get(SINK_PARALLELISM))
                .build();
    }

    private ClickHouseReadOptions getReadOptions(ReadableConfig config) {
        return new ClickHouseReadOptions.Builder()
                .withUrl(config.get(URL))
                .withUsername(config.get(USERNAME))
                .withPassword(config.get(PASSWORD))
                .withDatabaseName(config.get(DATABASE_NAME))
                .withTableName(config.get(TABLE_NAME))
                .withUseLocal(config.get(USE_LOCAL))
                .withPartitionColumn(config.get(SCAN_PARTITION_COLUMN))
                .withPartitionNum(config.get(SCAN_PARTITION_NUM))
                .withPartitionLowerBound(config.get(SCAN_PARTITION_LOWER_BOUND))
                .withPartitionUpperBound(config.get(SCAN_PARTITION_UPPER_BOUND))
                .build();
    }

    @Nullable
    private LookupCache getLookupCache(ReadableConfig tableOptions) {
        LookupCache cache = null;
        if (tableOptions
                .get(LookupOptions.CACHE_TYPE)
                .equals(LookupOptions.LookupCacheType.PARTIAL)) {
            cache = DefaultLookupCache.fromConfig(tableOptions);
        }
        return cache;
    }
}
