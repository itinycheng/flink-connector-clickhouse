//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.flink.connector.clickhouse.internal.executor;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.connector.clickhouse.internal.ClickHouseStatementFactory;
import org.apache.flink.connector.clickhouse.internal.connection.ClickHouseConnectionProvider;
import org.apache.flink.connector.clickhouse.internal.converter.ClickHouseRowConverter;
import org.apache.flink.connector.clickhouse.internal.options.ClickHouseOptions;
import org.apache.flink.table.data.RowData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHousePreparedStatement;

import java.io.Serializable;
import java.sql.SQLException;

/** Executor interface for submitting data to ClickHouse. */
public interface ClickHouseExecutor extends Serializable {

    Logger LOG = LoggerFactory.getLogger(ClickHouseExecutor.class);

    void prepareStatement(ClickHouseConnection connection) throws SQLException;

    void prepareStatement(ClickHouseConnectionProvider connectionProvider) throws SQLException;

    void setRuntimeContext(RuntimeContext context);

    void addToBatch(RowData rowData) throws SQLException;

    void executeBatch() throws SQLException;

    void closeStatement() throws SQLException;

    default void attemptExecuteBatch(ClickHousePreparedStatement stmt, int maxRetries)
            throws SQLException {
        for (int i = 0; i <= maxRetries; i++) {
            try {
                stmt.executeBatch();
                return;
            } catch (Exception exception) {
                LOG.error("ClickHouse executeBatch error, retry times = {}", i, exception);
                if (i >= maxRetries) {
                    throw new SQLException(
                            String.format(
                                    "Attempt to execute batch failed, exhausted retry times = %d",
                                    maxRetries),
                            exception);
                }
                try {
                    Thread.sleep(1000 * i);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new SQLException(
                            "Unable to flush; interrupted while doing another attempt", ex);
                }
            }
        }
    }

    static ClickHouseExecutor createClickHouseExecutor(
            String tableName,
            String[] fieldNames,
            String[] keyFields,
            ClickHouseRowConverter converter,
            ClickHouseOptions options) {
        if (keyFields.length > 0) {
            return createUpsertExecutor(tableName, fieldNames, keyFields, converter, options);
        } else {
            return createBatchExecutor(tableName, fieldNames, converter, options);
        }
    }

    static ClickHouseBatchExecutor createBatchExecutor(
            String tableName,
            String[] fieldNames,
            ClickHouseRowConverter converter,
            ClickHouseOptions options) {
        String sql = ClickHouseStatementFactory.getInsertIntoStatement(tableName, fieldNames);
        return new ClickHouseBatchExecutor(sql, converter, options);
    }

    static ClickHouseUpsertExecutor createUpsertExecutor(
            String tableName,
            String[] fieldNames,
            String[] keyFields,
            ClickHouseRowConverter converter,
            ClickHouseOptions options) {
        String insertSql = ClickHouseStatementFactory.getInsertIntoStatement(tableName, fieldNames);
        String updateSql =
                ClickHouseStatementFactory.getUpdateStatement(
                        tableName, fieldNames, keyFields, null);
        String deleteSql =
                ClickHouseStatementFactory.getDeleteStatement(tableName, keyFields, null);
        return new ClickHouseUpsertExecutor(insertSql, updateSql, deleteSql, converter, options);
    }
}
