package org.apache.flink.connector.clickhouse.catalog;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.connector.clickhouse.config.ClickHouseConfig.IDENTIFIER;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.DEFAULT_DATABASE;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.PASSWORD;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.URL;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.USERNAME;
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
        options.add(DEFAULT_DATABASE);
        options.add(USERNAME);
        options.add(PASSWORD);
        options.add(URL);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PROPERTY_VERSION);
        return options;
    }

    @Override
    public Catalog createCatalog(Context context) {
        final FactoryUtil.CatalogFactoryHelper helper =
                FactoryUtil.createCatalogFactoryHelper(this, context);
        helper.validate();

        return new ClickHouseCatalog(
                context.getName(),
                helper.getOptions().get(DEFAULT_DATABASE),
                helper.getOptions().get(URL),
                helper.getOptions().get(USERNAME),
                helper.getOptions().get(PASSWORD),
                ((Configuration) helper.getOptions()).toMap());
    }
}
