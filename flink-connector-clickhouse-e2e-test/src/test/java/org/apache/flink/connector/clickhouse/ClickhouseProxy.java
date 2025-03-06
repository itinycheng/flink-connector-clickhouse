package org.apache.flink.connector.clickhouse;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/** A proxy for Clickhouse to execute SQLs and check results. */
public class ClickhouseProxy {
    private final String jdbcUrl;
    private final String username;
    private final String password;
    private static final Logger logger = LoggerFactory.getLogger(ClickhouseProxy.class);
    Statement statement;
    Connection connection;

    ClickhouseProxy(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    public void connect() {
        try {
            if (connection == null) {
                Properties properties = new Properties();
                properties.put("username", username);
                properties.put("password", password);
                connection = DriverManager.getConnection(jdbcUrl, properties);
                statement = connection.createStatement();
            }
        } catch (Exception e) {
            logger.error("Failed to connect to clickhouse", e);
        }
    }

    public void execute(String sql) throws SQLException {
        connect();
        statement.execute(sql);
    }

    private void checkResult(List<String> expectedResult, String table, List<String> fields)
            throws Exception {
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

            results.add(String.join(",", result));
        }
        Collections.sort(results);
        Collections.sort(expectedResult);
        Assert.assertArrayEquals(expectedResult.toArray(), results.toArray());
    }

    public void checkResultWithTimeout(
            List<String> expectedResult, String table, List<String> fields, long timeout)
            throws Exception {
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
