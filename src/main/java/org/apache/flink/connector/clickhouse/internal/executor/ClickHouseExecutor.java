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

import ru.yandex.clickhouse.ClickHouseConnection;

import java.io.Serializable;
import java.sql.SQLException;

/** Executor interface for submitting data to ClickHouse. */
public interface ClickHouseExecutor extends Serializable {

    void prepareStatement(ClickHouseConnection connection) throws SQLException;

    void prepareStatement(ClickHouseConnectionProvider connectionProvider) throws SQLException;

    void setRuntimeContext(RuntimeContext context);

    void addToBatch(RowData rowData) throws SQLException;

    void executeBatch() throws SQLException;

    void closeStatement() throws SQLException;

    static ClickHouseExecutor createClickHouseExecutor(
            String tableName,
            String[] fieldNames,
            String[] keyFields,
            ClickHouseRowConverter converter,
            ClickHouseOptions options) {
        if (keyFields.length > 0) {
            return createUpsertExecutor(tableName, fieldNames, keyFields, converter, options);
        } else {
            return createBatchExecutor(tableName, fieldNames, converter);
        }
    }

    static ClickHouseBatchExecutor createBatchExecutor(
            String tableName, String[] fieldNames, ClickHouseRowConverter converter) {
        String sql = ClickHouseStatementFactory.getInsertIntoStatement(tableName, fieldNames);
        return new ClickHouseBatchExecutor(sql, converter);
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
