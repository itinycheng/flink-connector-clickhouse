//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.flink.connector.clickhouse.internal.executor;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.connector.clickhouse.internal.connection.ClickHouseConnectionProvider;
import org.apache.flink.connector.clickhouse.internal.converter.ClickHouseRowConverter;
import org.apache.flink.connector.clickhouse.internal.options.ClickHouseOptions;
import org.apache.flink.table.data.RowData;

import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHousePreparedStatement;

import java.sql.SQLException;
import java.util.Arrays;

/** ClickHouse's upsert executor. */
public class ClickHouseUpsertExecutor implements ClickHouseExecutor {

    private static final long serialVersionUID = 1L;

    private transient ClickHousePreparedStatement insertStmt;

    private transient ClickHousePreparedStatement updateStmt;

    private transient ClickHousePreparedStatement deleteStmt;

    private final String insertSql;

    private final String updateSql;

    private final String deleteSql;

    private final ClickHouseRowConverter converter;

    private final int maxRetries;

    public ClickHouseUpsertExecutor(
            String insertSql,
            String updateSql,
            String deleteSql,
            ClickHouseRowConverter converter,
            ClickHouseOptions options) {
        this.insertSql = insertSql;
        this.updateSql = updateSql;
        this.deleteSql = deleteSql;
        this.converter = converter;
        this.maxRetries = options.getMaxRetries();
    }

    @Override
    public void prepareStatement(ClickHouseConnection connection) throws SQLException {
        this.insertStmt = (ClickHousePreparedStatement) connection.prepareStatement(this.insertSql);
        this.updateStmt = (ClickHousePreparedStatement) connection.prepareStatement(this.updateSql);
        this.deleteStmt = (ClickHousePreparedStatement) connection.prepareStatement(this.deleteSql);
    }

    @Override
    public void prepareStatement(ClickHouseConnectionProvider connectionProvider) {
        throw new UnsupportedOperationException(
                "Please use `prepareStatement(ClickHouseConnection connection)` instead.");
    }

    @Override
    public void setRuntimeContext(RuntimeContext context) {}

    @Override
    public void addToBatch(RowData record) throws SQLException {
        switch (record.getRowKind()) {
            case INSERT:
                converter.toExternal(record, insertStmt);
                insertStmt.addBatch();
                break;
            case UPDATE_AFTER:
                converter.toExternal(record, updateStmt);
                updateStmt.addBatch();
                break;
            case DELETE:
                converter.toExternal(record, deleteStmt);
                deleteStmt.addBatch();
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
        for (ClickHousePreparedStatement clickHousePreparedStatement :
                Arrays.asList(insertStmt, updateStmt, deleteStmt)) {
            if (clickHousePreparedStatement != null) {
                attemptExecuteBatch(clickHousePreparedStatement, maxRetries);
            }
        }
    }

    @Override
    public void closeStatement() throws SQLException {
        for (ClickHousePreparedStatement clickHousePreparedStatement :
                Arrays.asList(insertStmt, updateStmt, deleteStmt)) {
            if (clickHousePreparedStatement != null) {
                clickHousePreparedStatement.close();
            }
        }
    }

    @Override
    public String toString() {
        return "ClickHouseUpsertExecutor{"
                + "insertStmt="
                + insertStmt
                + ", updateStmt="
                + updateStmt
                + ", deleteStmt="
                + deleteStmt
                + ", insertSql='"
                + insertSql
                + '\''
                + ", updateSql='"
                + updateSql
                + '\''
                + ", deleteSql='"
                + deleteSql
                + '\''
                + ", converter="
                + converter
                + ", maxRetries="
                + maxRetries
                + '}';
    }
}
