//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package com.tiny.flink.connector.clickhouse.internal.executor;

import com.tiny.flink.connector.clickhouse.internal.ClickHouseStatementFactory;
import com.tiny.flink.connector.clickhouse.internal.connection.ClickHouseConnectionProvider;
import com.tiny.flink.connector.clickhouse.internal.converter.ClickHouseRowConverter;
import com.tiny.flink.connector.clickhouse.internal.options.ClickHouseOptions;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.table.data.RowData;
import ru.yandex.clickhouse.ClickHouseConnection;

import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.Optional;

/**
 * @author tiger
 */
public interface ClickHouseExecutor extends Serializable {

    void prepareStatement(ClickHouseConnection var1) throws SQLException;

    void prepareStatement(ClickHouseConnectionProvider var1) throws SQLException;

    void setRuntimeContext(RuntimeContext var1);

    void addBatch(RowData var1) throws IOException;

    void executeBatch() throws IOException;

    void closeStatement() throws SQLException;

    static ClickHouseUpsertExecutor createUpsertExecutor(String tableName, String[] fieldNames, String[] keyFields, ClickHouseRowConverter converter, ClickHouseOptions options) {
        String insertSql = ClickHouseStatementFactory.getInsertIntoStatement(tableName, fieldNames);
        String updateSql = ClickHouseStatementFactory.getUpdateStatement(tableName, fieldNames, keyFields, Optional.empty());
        String deleteSql = ClickHouseStatementFactory.getDeleteStatement(tableName, keyFields, Optional.empty());
        return new ClickHouseUpsertExecutor(insertSql, updateSql, deleteSql, converter, options);
    }
}
