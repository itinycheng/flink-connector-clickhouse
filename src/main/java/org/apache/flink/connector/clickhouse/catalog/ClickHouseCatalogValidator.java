package org.apache.flink.connector.clickhouse.catalog;

import org.apache.flink.table.descriptors.CatalogDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;

import static org.apache.flink.connector.clickhouse.config.ClickHouseConfig.IDENTIFIER;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfig.PASSWORD;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfig.URL;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfig.USERNAME;

/** ClickHouse catalog validator. */
public class ClickHouseCatalogValidator extends CatalogDescriptorValidator {

    @Override
    public void validate(DescriptorProperties properties) {
        super.validate(properties);
        properties.validateValue(CATALOG_TYPE, IDENTIFIER, false);
        properties.validateString(URL, false, 1);
        properties.validateString(USERNAME, false, 1);
        properties.validateString(PASSWORD, false, 1);
    }
}
