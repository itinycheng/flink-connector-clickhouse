package org.apache.flink.connector.clickhouse;

import com.clickhouse.jdbc.ClickHouseConnection;

import com.clickhouse.jdbc.ClickHouseDriver;

import com.clickhouse.jdbc.ClickHouseStatement;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.testframe.container.FlinkContainers;
import org.apache.flink.connector.testframe.container.FlinkContainersSettings;
import org.apache.flink.connector.testframe.container.TestcontainersSettings;

import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.test.resources.ResourceTestUtils;
import org.apache.flink.test.util.SQLJobSubmission;

import org.apache.flink.test.util.TestUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class ClickhouseE2ECase extends FlinkContainerTestEnviroment {

    private static final Logger logger = LoggerFactory.getLogger(ClickhouseE2ECase.class);

    ClickhouseProxy proxy;


    @Test
    public void testSink() throws Exception {
        String jdbcUrl = String.format("jdbc:clickhouse://%s:%s/default", "clickhouse",
                "8123");

        proxy = new ClickhouseProxy(CLICKHOUSE_CONTAINER.getJdbcUrl(), CLICKHOUSE_CONTAINER.getUsername(),CLICKHOUSE_CONTAINER.getPassword());
        proxy.execute(
                "create table test (id Int32, name String) engine = Memory");
        proxy.execute("create table test_insert (id Int32, name String) engine = Memory");
        proxy.execute("insert into test values (1, 'test');");
        proxy.execute("insert into test values (2, 'kiki');");
        List<String> sqlLines = new ArrayList<>();
        sqlLines.add("create table clickhouse_test (id int, name varchar) with ('connector' = 'clickhouse',\n"
                + "  'url' = '" + jdbcUrl + "',\n"
                + "  'table-name' = 'test',\n"
                + "  'username'='test_username',\n"
                + "  'password'='test_password'\n"
                + ");");
        sqlLines.add("create table test (id int, name varchar) with ('connector' = 'clickhouse',\n"
                + "  'url' = '" + jdbcUrl + "',\n"
                + "  'username'='test_username',\n"
                + "  'password'='test_password',\n"
                + "  'table-name' = 'test_insert');");
        sqlLines.add("insert into test select * from clickhouse_test;");

        submitSQLJob(sqlLines, SQL_CONNECTOR_CLICKHOUSE_JAR, CLICKHOUSE_JDBC_JAR, HTTPCORE_JAR, HTTPCLIENT_JAR, HTTPCLIENT_H2_JAR);
        waitUntilJobRunning(Duration.of(1, ChronoUnit.MINUTES));
        List<String> expectedResult = Arrays.asList("1,test","2,kiki");
        proxy.checkResultWithTimeout(expectedResult, "test_insert", Arrays.asList("id", "name"), 60000);
    }




    @After
    public void tearDown() throws SQLException {
        CLICKHOUSE_CONTAINER.stop();
    }

}
