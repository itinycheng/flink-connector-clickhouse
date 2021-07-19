//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package com.tiny.flink.connector.clickhouse.internal.executor;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.table.data.RowData;

import com.tiny.flink.connector.clickhouse.internal.connection.ClickHouseConnectionProvider;
import com.tiny.flink.connector.clickhouse.internal.converter.ClickHouseRowConverter;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHousePreparedStatement;

import java.sql.SQLException;

/** ClickHouse's batch executor. */
public class ClickHouseBatchExecutor implements ClickHouseExecutor {

    private static final long serialVersionUID = 1L;

    private final String sql;

    private final ClickHouseRowConverter converter;

    private transient ClickHousePreparedStatement statement;

    private transient ClickHouseConnectionProvider connectionProvider;

    public ClickHouseBatchExecutor(String sql, ClickHouseRowConverter converter) {
        this.sql = sql;
        this.converter = converter;
    }

    @Override
    public void prepareStatement(ClickHouseConnection connection) throws SQLException {
        statement = (ClickHousePreparedStatement) connection.prepareStatement(sql);
    }

    @Override
    public void prepareStatement(ClickHouseConnectionProvider connectionProvider)
            throws SQLException {
        this.connectionProvider = connectionProvider;
        prepareStatement(connectionProvider.getConnection());
    }

    @Override
    public void setRuntimeContext(RuntimeContext context) {}

    @Override
    public synchronized void addToBatch(RowData record) throws SQLException {
        // We should add records to batch, no matter what RowKind is
        switch (record.getRowKind()) {
            case INSERT:
            case UPDATE_AFTER:
            case DELETE:
                converter.toExternal(record, statement);
                statement.addBatch();
                break;
            case UPDATE_BEFORE:
                break;
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Unknown row kind, the supported row kinds is: INSERT, UPDATE_BEFORE, UPDATE_AFTER, DELETE, but get: %s.",
                                record.getRowKind()));
        }
    }

    @Override
    public void executeBatch() throws SQLException {
        statement.executeBatch();
    }

    @Override
    public void closeStatement() throws SQLException {
        if (statement != null) {
            statement.close();
            statement = null;
        }
    }

    @Override
    public String toString() {
        return "ClickHouseBatchExecutor{"
                + "sql='"
                + sql
                + '\''
                + ", converter="
                + converter
                + ", statement="
                + statement
                + ", connectionProvider="
                + connectionProvider
                + '}';
    }
}
