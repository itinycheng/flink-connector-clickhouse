package org.apache.flink.connector.clickhouse;

import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** End-to-end test for Clickhouse. */
public class ClickhouseE2ECase extends FlinkContainerTestEnvironment {

    private static final Logger logger = LoggerFactory.getLogger(ClickhouseE2ECase.class);

    ClickhouseProxy proxy;

    @Test
    public void testSink() throws Exception {
        String jdbcUrl = String.format("jdbc:clickhouse://%s:%s/default", "clickhouse", "8123");

        proxy =
                new ClickhouseProxy(
                        CLICKHOUSE_CONTAINER.getJdbcUrl(),
                        CLICKHOUSE_CONTAINER.getUsername(),
                        CLICKHOUSE_CONTAINER.getPassword());
        proxy.execute(
                "create table test (id Int32, name String, float32_column Float32, date_column Date,datetime_column DateTime, array_column Array(Int32)) engine = Memory");
        proxy.execute(
                "create table test_insert (id Int32, name String, float32_column Float32, date_column Date,datetime_column DateTime, array_column Array(Int32)) engine = Memory; ");
        proxy.execute(
                "INSERT INTO test (id, name, float32_column, date_column, datetime_column, array_column) VALUES (1, 'Name1', 1.1, '2022-01-01', '2022-01-01 00:00:00', [1, 2, 3]);");
        proxy.execute(
                "INSERT INTO test (id, name, float32_column, date_column, datetime_column, array_column) VALUES (2, 'Name2', 2.2, '2022-01-02', '2022-01-02 01:00:00', [4, 5, 6]);");
        proxy.execute(
                "INSERT INTO test (id, name, float32_column, date_column, datetime_column, array_column) VALUES (3, 'Name3', 3.3, '2022-01-03', '2022-01-03 02:00:00', [7, 8, 9]);");
        proxy.execute(
                "INSERT INTO test (id, name, float32_column, date_column, datetime_column, array_column) VALUES (4, 'Name4', 4.4, '2022-01-04', '2022-01-04 03:00:00', [10, 11, 12]);");
        proxy.execute(
                "INSERT INTO test (id, name, float32_column, date_column, datetime_column, array_column) VALUES (5, 'Name5', 5.5, '2022-01-05', '2022-01-05 04:00:00', [13, 14, 15]);");
        // proxy.execute("insert into test values (2, 'kiki');");
        List<String> sqlLines = new ArrayList<>();
        sqlLines.add(
                "create table clickhouse_test (id int, name varchar,float32_column FLOAT,\n"
                        + "    datetime_column TIMESTAMP(3),\n"
                        + "    array_column ARRAY<INT>) with ('connector' = 'clickhouse',\n"
                        + "  'url' = '"
                        + jdbcUrl
                        + "',\n"
                        + "  'table-name' = 'test',\n"
                        + "  'username'='test_username',\n"
                        + "  'password'='test_password'\n"
                        + ");");
        sqlLines.add(
                "create table test (id int, name varchar,float32_column FLOAT,\n"
                        + "    datetime_column TIMESTAMP(3),\n"
                        + "    array_column ARRAY<INT>) with ('connector' = 'clickhouse',\n"
                        + "  'url' = '"
                        + jdbcUrl
                        + "',\n"
                        + "  'table-name' = 'test_insert',\n"
                        + "  'username'='test_username',\n"
                        + "  'password'='test_password'\n"
                        + ");");
        sqlLines.add("insert into test select * from clickhouse_test;");

        submitSQLJob(
                sqlLines,
                SQL_CONNECTOR_CLICKHOUSE_JAR,
                CLICKHOUSE_JDBC_JAR,
                HTTPCORE_JAR,
                HTTPCLIENT_JAR,
                HTTPCLIENT_H2_JAR);
        waitUntilJobRunning(Duration.of(1, ChronoUnit.MINUTES));
        List<String> expectedResult =
                Arrays.asList(
                        "1,Name1,1.1,2022-01-01 00:00:00,[1,2,3]",
                        "2,Name2,2.2,2022-01-02 01:00:00,[4,5,6]",
                        "3,Name3,3.3,2022-01-03 02:00:00,[7,8,9]",
                        "4,Name4,4.4,2022-01-04 03:00:00,[10,11,12]",
                        "5,Name5,5.5,2022-01-05 04:00:00,[13,14,15]");
        proxy.checkResultWithTimeout(
                expectedResult,
                "test_insert",
                Arrays.asList("id", "name", "float32_column", "datetime_column", "array_column"),
                60000);
    }

    @After
    public void tearDown() throws SQLException {
        CLICKHOUSE_CONTAINER.stop();
    }
}
