package org.apache.flink.connector.clickhouse;

import com.clickhouse.jdbc.ClickHouseConnection;

import com.clickhouse.jdbc.ClickHouseDriver;

import com.clickhouse.jdbc.ClickHouseStatement;

import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.connector.testframe.container.FlinkContainers;
import org.apache.flink.connector.testframe.container.FlinkContainersSettings;
import org.apache.flink.connector.testframe.container.TestcontainersSettings;

import org.apache.flink.test.util.SQLJobSubmission;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;


public class ClickhouseE2ECase {

    private static final Logger logger = LoggerFactory.getLogger(ClickhouseE2ECase.class);

    static final ClickHouseContainer CLICKHOUSE_CONTAINER =
            new ClickHouseContainer("clickhouse/clickhouse-server:latest")
                    .withLogConsumer(new Slf4jLogConsumer(logger));

    public static final Network NETWORK = Network.newNetwork();

    private static final TestcontainersSettings TESTCONTAINERS_SETTINGS =
            TestcontainersSettings.builder()
                    .logger(logger)
                    .network(NETWORK)
                    .dependsOn(CLICKHOUSE_CONTAINER)
                    .build();
    // .baseImage("flink:1.19.0-scala_2.12")
    public static final FlinkContainers FLINK =
            FlinkContainers.builder()
                    .withFlinkContainersSettings(
                            FlinkContainersSettings.builder()
                                    .numTaskManagers(1)
                                    .setConfigOption(JobManagerOptions.ADDRESS,"jobmanager").baseImage("flink:1.19.0-scala_2.12").build())
                    .withTestcontainersSettings(
                            TESTCONTAINERS_SETTINGS)
                    .build();

    ClickHouseConnection connection;

    @Before
    public void setUp() throws Exception {
        String properties = "jobmanager.rpc.address: jobmanager";
        FLINK.getJobManager().withLabel("com.testcontainers.allow-filesystem-access","true");
        FLINK.getJobManager().withNetworkAliases("jobmanager")
                .withExposedPorts(8081).withEnv("FLINK_PROPERTIES", properties)
                .withExtraHost("host.docker.internal", "host-gateway");

        FLINK.getTaskManagers().forEach(tm -> {tm.withLabel("com.testcontainers.allow-filesystem-access","true");
            tm.withNetworkAliases("taskmanager");
            tm.withEnv("FLINK_PROPERTIES", properties);
            tm.dependsOn(FLINK.getJobManager());
            tm.withExtraHost("host.docker.internal", "host-gateway");});
        logger.info("starting containers");
        FLINK.start();
        // FLINK.getTaskManagers().forEach(tm -> tm.withLabel("com.testcontainers.allow-filesystem-access","true"));
        ClickHouseDriver driver = new ClickHouseDriver();
        connection = driver.connect(CLICKHOUSE_CONTAINER.getJdbcUrl(), null);

    }

    @Test
    public void testSink() throws Exception {
        ClickHouseStatement statement = connection.createStatement();
        statement.execute("create table test (id Int32, name String) engine = Memory");
        statement.execute("create table test_insert (id Int32, name String) engine = Memory");

        statement.execute("insert into test values (1, 'test')");
        List<String> sqlLines = new ArrayList<>();
        sqlLines.add("create table clickhouse_test (id int, name varchar) with ('connector' = 'clickhouse',\n"
                + "  'uri' = 'jdbc:clickhouse://clickhouse:8030',\n"
                + "  'table' = 'test');");
        sqlLines.add("create table test (id int, name varchar) with ('connector' = 'clickhouse',\n"
                + "  'uri' = 'jdbc:clickhouse://clickhouse:8030',\n"
                + "  'table' = 'test_insert');");
        sqlLines.add("insert into clickhouse_test select * from test;");
        executeSqlStatements(sqlLines);
        ResultSet resultSet = statement.executeQuery("select * from test_insert");

        while (resultSet.next()) {
            int id = resultSet.getInt("id");
            String name = resultSet.getString("name");
            //TODO
        }
    }

    private static void executeSqlStatements(final List<String> sqlLines) throws Exception {

        FLINK.submitSQLJob(
                new SQLJobSubmission.SQLJobSubmissionBuilder(sqlLines)
                        .build());
    }

    @After
    public void tearDown() throws SQLException {
        CLICKHOUSE_CONTAINER.stop();
        if(connection != null) {
            connection.close();
        }
    }

}
