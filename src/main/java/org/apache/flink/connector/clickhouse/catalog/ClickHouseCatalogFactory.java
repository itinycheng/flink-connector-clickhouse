package org.apache.flink.connector.clickhouse.catalog;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.connector.clickhouse.config.ClickHouseConfig.IDENTIFIER;
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
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.URL;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.USERNAME;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.USE_LOCAL;
import static org.apache.flink.table.factories.FactoryUtil.PROPERTY_VERSION;

/** Factory for {@link ClickHouseCatalog}. */
public class ClickHouseCatalogFactory implements CatalogFactory {

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(URL);
        options.add(USERNAME);
        options.add(PASSWORD);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PROPERTY_VERSION);
        options.add(DATABASE_NAME);
        options.add(USE_LOCAL);
        options.add(CATALOG_IGNORE_PRIMARY_KEY);

        options.add(SINK_BATCH_SIZE);
        options.add(SINK_FLUSH_INTERVAL);
        options.add(SINK_MAX_RETRIES);
        options.add(SINK_PARTITION_STRATEGY);
        options.add(SINK_PARTITION_KEY);
        options.add(SINK_IGNORE_DELETE);
        options.add(SINK_PARALLELISM);

        options.add(SCAN_PARTITION_COLUMN);
        options.add(SCAN_PARTITION_NUM);
        options.add(SCAN_PARTITION_LOWER_BOUND);
        options.add(SCAN_PARTITION_UPPER_BOUND);
        return options;
    }

    @Override
    public Catalog createCatalog(Context context) {
        final FactoryUtil.CatalogFactoryHelper helper =
                FactoryUtil.createCatalogFactoryHelper(this, context);
        helper.validate();

        return new ClickHouseCatalog(
                context.getName(),
                helper.getOptions().get(DATABASE_NAME),
                helper.getOptions().get(URL),
                helper.getOptions().get(USERNAME),
                helper.getOptions().get(PASSWORD),
                ((Configuration) helper.getOptions()).toMap());
    }
}
