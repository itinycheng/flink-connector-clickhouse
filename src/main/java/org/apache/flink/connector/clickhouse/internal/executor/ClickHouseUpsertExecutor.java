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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.function.Function;

/** ClickHouse's upsert executor. */
public class ClickHouseUpsertExecutor implements ClickHouseExecutor {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseShardOutputFormat.class);

    private final String[] keyFields;

    private final String insertSql;

    private final String updateSql;

    private final String deleteSql;

    private final String existSql;

    private final ClickHouseRowConverter insertConverter;

    private final ClickHouseRowConverter updateConverter;

    private final ClickHouseRowConverter deleteConverter;

    private final ClickHouseRowConverter existConverter;

    private final Function<RowData, RowData> updateExtractor;

    private final Function<RowData, RowData> deleteExtractor;

    private final Function<RowData, RowData> existExtractor;

    private final int maxRetries;

    private transient ClickHousePreparedStatement insertStmt;

    private transient ClickHousePreparedStatement updateStmt;

    private transient ClickHousePreparedStatement deleteStmt;

    private transient ClickHousePreparedStatement existStmt;

    private transient ClickHouseConnectionProvider connectionProvider;

    public ClickHouseUpsertExecutor(
            String[] keyFields,
            String insertSql,
            String updateSql,
            String deleteSql,
            String existSql,
            ClickHouseRowConverter insertConverter,
            ClickHouseRowConverter updateConverter,
            ClickHouseRowConverter deleteConverter,
            ClickHouseRowConverter existConverter,
            Function<RowData, RowData> updateExtractor,
            Function<RowData, RowData> deleteExtractor,
            Function<RowData, RowData> existExtractor,
            ClickHouseDmlOptions options) {
        this.keyFields = keyFields;
        this.insertSql = insertSql;
        this.updateSql = updateSql;
        this.deleteSql = deleteSql;
        this.existSql = existSql;
        this.insertConverter = insertConverter;
        this.updateConverter = updateConverter;
        this.deleteConverter = deleteConverter;
        this.existConverter = existConverter;
        this.updateExtractor = updateExtractor;
        this.deleteExtractor = deleteExtractor;
        this.existExtractor = existExtractor;
        this.maxRetries = options.getMaxRetries();
    }

    @Override
    public void prepareStatement(ClickHouseConnection connection) throws SQLException {
        this.insertStmt = (ClickHousePreparedStatement) connection.prepareStatement(this.insertSql);
        this.updateStmt = (ClickHousePreparedStatement) connection.prepareStatement(this.updateSql);
        this.deleteStmt = (ClickHousePreparedStatement) connection.prepareStatement(this.deleteSql);
        this.existStmt = (ClickHousePreparedStatement) connection.prepareStatement(this.existSql);
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
            case UPDATE_AFTER:
                if (exist(record)) {
                    updateConverter.toExternal(updateExtractor.apply(record), updateStmt);
                    updateStmt.addBatch();
                } else {
                    insertConverter.toExternal(record, insertStmt);
                    insertStmt.addBatch();
                }
                break;
            case DELETE:
                deleteConverter.toExternal(deleteExtractor.apply(record), deleteStmt);
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

    private boolean exist(RowData record) throws SQLException {
        existConverter.toExternal(existExtractor.apply(record), existStmt);
        try (ResultSet resultSet = existStmt.executeQuery()) {
            return resultSet.next();
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
    public void closeStatement() {
        for (ClickHousePreparedStatement clickHousePreparedStatement :
                Arrays.asList(insertStmt, updateStmt, deleteStmt)) {
            if (clickHousePreparedStatement != null) {
                try {
                    clickHousePreparedStatement.close();
                } catch (SQLException exception) {
                    LOG.warn("ClickHouse upsert statement could not be closed.", exception);
                }
            }
        }
    }

    @Override
    public String toString() {
        return "ClickHouseUpsertExecutor{"
                + "insertSql='"
                + insertSql
                + '\''
                + ", updateSql='"
                + updateSql
                + '\''
                + ", deleteSql='"
                + deleteSql
                + '\''
                + ", maxRetries="
                + maxRetries
                + ", connectionProvider="
                + connectionProvider
                + '}';
    }
}
