package org.apache.flink.connector.clickhouse.catalog;

import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.CatalogFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.connector.clickhouse.config.ClickHouseConfig.IDENTIFIER;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfig.PASSWORD;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfig.URL;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfig.USERNAME;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_DEFAULT_DATABASE;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_TYPE;

/** Factory for {@link ClickHouseCatalog}. */
public class ClickHouseCatalogFactory implements CatalogFactory {

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> context = new HashMap<>(2);
        context.put(CATALOG_TYPE, IDENTIFIER);
        context.put(CATALOG_PROPERTY_VERSION, "1");
        return context;
    }

    @Override
    public List<String> supportedProperties() {
        List<String> properties = new ArrayList<>();
        properties.add(CATALOG_DEFAULT_DATABASE);
        properties.add(URL);
        properties.add(USERNAME);
        properties.add(PASSWORD);
        return properties;
    }

    @Override
    public Catalog createCatalog(String name, Map<String, String> properties) {
        final DescriptorProperties props = getValidatedProperties(properties);

        return new ClickHouseCatalog(
                name,
                props.getString(CATALOG_DEFAULT_DATABASE),
                props.getString(URL),
                props.getString(USERNAME),
                props.getString(PASSWORD),
                properties);
    }

    private static DescriptorProperties getValidatedProperties(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putProperties(properties);
        new ClickHouseCatalogValidator().validate(descriptorProperties);
        return descriptorProperties;
    }
}
