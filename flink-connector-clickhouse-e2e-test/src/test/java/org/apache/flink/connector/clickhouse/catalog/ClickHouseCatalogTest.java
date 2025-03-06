package org.apache.flink.connector.clickhouse.catalog;

import org.apache.flink.connector.clickhouse.config.ClickHouseConfig;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.util.HashMap;
import java.util.Map;

/** unit test for ClickHouseCatalog. */
public class ClickHouseCatalogTest {
    private static final Logger log = LoggerFactory.getLogger(ClickHouseCatalogTest.class);

    public static final Network NETWORK = Network.newNetwork();

    static final ClickHouseContainer CLICKHOUSE_CONTAINER =
            new ClickHouseContainer("clickhouse/clickhouse-server:latest")
                    .withNetwork(NETWORK)
                    .withNetworkAliases("clickhouse")
                    .withExposedPorts(8123, 9000)
                    .withUsername("default")
                    .withPassword("pwd")
                    .withLogConsumer(new Slf4jLogConsumer(log));

    @Test
    public void test() throws TableNotExistException {
        Map<String, String> properties = new HashMap<>();
        properties.put(ClickHouseConfig.URL, CLICKHOUSE_CONTAINER.getJdbcUrl());
        properties.put(ClickHouseConfig.USERNAME, CLICKHOUSE_CONTAINER.getUsername());
        properties.put(ClickHouseConfig.PASSWORD, CLICKHOUSE_CONTAINER.getPassword());
        ClickHouseCatalog catalog = new ClickHouseCatalog("default_catalog", properties);
        catalog.open();
        catalog.listDatabases();
        catalog.getTable(ObjectPath.fromString("system.disks"));
    }

    @Before
    public void setUp() {
        CLICKHOUSE_CONTAINER.start();
    }

    @After
    public void tearDown() {
        CLICKHOUSE_CONTAINER.stop();
    }
}
