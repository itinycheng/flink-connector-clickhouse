//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package com.tiny.flink.connector.clickhouse.internal.executor;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.table.data.RowData;

import com.tiny.flink.connector.clickhouse.internal.ClickHouseStatementFactory;
import com.tiny.flink.connector.clickhouse.internal.connection.ClickHouseConnectionProvider;
import com.tiny.flink.connector.clickhouse.internal.converter.ClickHouseRowConverter;
import com.tiny.flink.connector.clickhouse.internal.options.ClickHouseOptions;
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
