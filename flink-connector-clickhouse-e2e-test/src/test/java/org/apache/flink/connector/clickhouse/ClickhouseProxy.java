package org.apache.flink.connector.clickhouse;

import com.clickhouse.jdbc.ClickHouseConnection;
import com.clickhouse.jdbc.ClickHouseDataSource;
import com.clickhouse.jdbc.ClickHouseDriver;
import com.clickhouse.jdbc.ClickHouseStatement;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.org.apache.commons.lang3.StringUtils;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class ClickhouseProxy {
    private String jdbcUrl;
    private String username;
    private String password;
    private static final Logger logger = LoggerFactory.getLogger(ClickhouseProxy.class);
    ClickHouseDriver driver;
    ClickHouseStatement statement;
    ClickHouseConnection connection;
    ClickhouseProxy(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.driver =  new ClickHouseDriver();

    }

    public void connect() {
        try {
            if (connection == null) {
                Properties properties = new Properties();
                properties.put("username", username);
                properties.put("password", password);
                ClickHouseDataSource clickHouseDataSource = new ClickHouseDataSource(jdbcUrl, properties);
                connection = clickHouseDataSource.getConnection(username,password);
                statement =  connection.createStatement();
            }
        } catch (Exception e) {
            logger.error("Failed to connect to clickhouse", e);
        }
    }

    public void execute(String sql) throws SQLException {
        connect();
        statement.execute(sql);
    }

    private void checkResult(List<String> expectedResult, String table, List<String> fields) throws Exception {
        connect();
        List<String> results = new ArrayList<>();
        ResultSet resultSet = statement.executeQuery("select * from " + table);
        while (resultSet.next()) {
            List<String> result = new ArrayList<>();
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);
                    if (!fields.contains(columnName)) {
                        continue;
                    }
                    String columnType = metaData.getColumnTypeName(i);
                    switch (columnType) {
                        case "Array":
                            Array array = resultSet.getArray(i);
                            result.add(array.toString());
                            break;
                        case "Timestamp":
                            Timestamp timestamp = resultSet.getTimestamp(i);
                            result.add(timestamp.toString());
                            break;
                        default:
                            String value = resultSet.getString(i);
                            result.add(value);
                            break;
                    }
                }

            results.add(result.stream().collect(Collectors.joining(",")));
        }
        Collections.sort(results);
        Collections.sort(expectedResult);
        Assert.assertArrayEquals(expectedResult.toArray(), results.toArray());
    }

    public void checkResultWithTimeout(List<String> expectedResult, String table, List<String> fields, long timeout) throws Exception {
                 long endTimeout = System.currentTimeMillis() + timeout;
                 boolean result = false;
                 while (System.currentTimeMillis() < endTimeout) {
                     try {
                         checkResult(expectedResult, table, fields);
                         result = true;
                         break;
                     } catch (AssertionError | SQLException throwable) {
                         Thread.sleep(1000L);
                     }
                 }
                 if (!result) {
                     checkResult(expectedResult, table, fields);
                 }

    }
}
