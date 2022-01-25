//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.flink.connector.clickhouse.internal.executor;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.connector.clickhouse.internal.ClickHouseShardOutputFormat;
import org.apache.flink.connector.clickhouse.internal.connection.ClickHouseConnectionProvider;
import org.apache.flink.connector.clickhouse.internal.converter.ClickHouseRowConverter;
import org.apache.flink.connector.clickhouse.internal.options.ClickHouseDmlOptions;
import org.apache.flink.table.data.RowData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHousePreparedStatement;

import java.sql.SQLException;

/** ClickHouse's batch executor. */
public class ClickHouseBatchExecutor implements ClickHouseExecutor {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseShardOutputFormat.class);

    private final String insertSql;

    private final ClickHouseRowConverter converter;

    private final int maxRetries;

    private transient ClickHousePreparedStatement statement;

    private transient ClickHouseConnectionProvider connectionProvider;

    public ClickHouseBatchExecutor(
            String insertSql, ClickHouseRowConverter converter, ClickHouseDmlOptions options) {
        this.insertSql = insertSql;
        this.converter = converter;
        this.maxRetries = options.getMaxRetries();
    }

    @Override
    public void prepareStatement(ClickHouseConnection connection) throws SQLException {
        statement = (ClickHousePreparedStatement) connection.prepareStatement(insertSql);
    }

    @Override
    public void prepareStatement(ClickHouseConnectionProvider connectionProvider)
            throws SQLException {
        this.connectionProvider = connectionProvider;
        prepareStatement(connectionProvider.getOrCreateConnection());
    }

    @Override
    public void setRuntimeContext(RuntimeContext context) {}

    @Override
    public void addToBatch(RowData record) throws SQLException {
        switch (record.getRowKind()) {
            case INSERT:
                converter.toExternal(record, statement);
                statement.addBatch();
                break;
            case UPDATE_AFTER:
            case DELETE:
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
        attemptExecuteBatch(statement, maxRetries);
    }

    @Override
    public void closeStatement() {
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException exception) {
                LOG.warn("ClickHouse batch statement could not be closed.", exception);
            } finally {
                statement = null;
            }
        }
    }

    @Override
    public String toString() {
        return "ClickHouseBatchExecutor{"
                + "insertSql='"
                + insertSql
                + '\''
                + ", maxRetries="
                + maxRetries
                + ", connectionProvider="
                + connectionProvider
                + '}';
    }
}
