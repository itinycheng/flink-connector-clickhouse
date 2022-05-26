//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.flink.connector.clickhouse.internal.executor;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.connector.clickhouse.internal.ClickHouseStatementFactory;
import org.apache.flink.connector.clickhouse.internal.connection.ClickHouseConnectionProvider;
import org.apache.flink.connector.clickhouse.internal.converter.ClickHouseRowConverter;
import org.apache.flink.connector.clickhouse.internal.options.ClickHouseDmlOptions;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHousePreparedStatement;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.IntStream;

import static org.apache.flink.table.data.RowData.createFieldGetter;

/** Executor interface for submitting data to ClickHouse. */
public interface ClickHouseExecutor extends Serializable {

    Logger LOG = LoggerFactory.getLogger(ClickHouseExecutor.class);

    void prepareStatement(ClickHouseConnection connection) throws SQLException;

    void prepareStatement(ClickHouseConnectionProvider connectionProvider) throws SQLException;

    void setRuntimeContext(RuntimeContext context);

    void addToBatch(RowData rowData) throws SQLException;

    void executeBatch() throws SQLException;

    void closeStatement();

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
                    Thread.sleep(1000L * i);
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
            String databaseName,
            String clusterName,
            String[] fieldNames,
            String[] keyFields,
            String[] partitionFields,
            LogicalType[] fieldTypes,
            ClickHouseDmlOptions options) {
        if (keyFields.length > 0) {
            return createUpsertExecutor(
                    tableName,
                    databaseName,
                    clusterName,
                    fieldNames,
                    keyFields,
                    partitionFields,
                    fieldTypes,
                    options);
        } else {
            return createBatchExecutor(tableName, fieldNames, fieldTypes, options);
        }
    }

    static ClickHouseBatchExecutor createBatchExecutor(
            String tableName,
            String[] fieldNames,
            LogicalType[] fieldTypes,
            ClickHouseDmlOptions options) {
        String insertSql = ClickHouseStatementFactory.getInsertIntoStatement(tableName, fieldNames);
        ClickHouseRowConverter converter = new ClickHouseRowConverter(RowType.of(fieldTypes));
        return new ClickHouseBatchExecutor(insertSql, converter, options);
    }

    static ClickHouseUpsertExecutor createUpsertExecutor(
            String tableName,
            String databaseName,
            String clusterName,
            String[] fieldNames,
            String[] keyFields,
            String[] partitionFields,
            LogicalType[] fieldTypes,
            ClickHouseDmlOptions options) {
        String insertSql = ClickHouseStatementFactory.getInsertIntoStatement(tableName, fieldNames);
        String updateSql =
                ClickHouseStatementFactory.getUpdateStatement(
                        tableName,
                        databaseName,
                        clusterName,
                        fieldNames,
                        keyFields,
                        partitionFields);
        String deleteSql =
                ClickHouseStatementFactory.getDeleteStatement(
                        tableName, databaseName, clusterName, keyFields);

        // Re-sort the order of fields to fit the sql statement.
        int[] delFields =
                Arrays.stream(keyFields)
                        .mapToInt(pk -> ArrayUtils.indexOf(fieldNames, pk))
                        .toArray();
        int[] updatableFields =
                IntStream.range(0, fieldNames.length)
                        .filter(idx -> !ArrayUtils.contains(keyFields, fieldNames[idx]))
                        .filter(idx -> !ArrayUtils.contains(partitionFields, fieldNames[idx]))
                        .toArray();
        int[] updFields = ArrayUtils.addAll(updatableFields, delFields);

        LogicalType[] delTypes =
                Arrays.stream(delFields).mapToObj(f -> fieldTypes[f]).toArray(LogicalType[]::new);
        LogicalType[] updTypes =
                Arrays.stream(updFields).mapToObj(f -> fieldTypes[f]).toArray(LogicalType[]::new);

        return new ClickHouseUpsertExecutor(
                insertSql,
                updateSql,
                deleteSql,
                new ClickHouseRowConverter(RowType.of(fieldTypes)),
                new ClickHouseRowConverter(RowType.of(updTypes)),
                new ClickHouseRowConverter(RowType.of(delTypes)),
                createExtractor(fieldTypes, updFields),
                createExtractor(fieldTypes, delFields),
                options);
    }

    static Function<RowData, RowData> createExtractor(LogicalType[] logicalTypes, int[] fields) {
        final RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[fields.length];
        for (int i = 0; i < fields.length; i++) {
            fieldGetters[i] = createFieldGetter(logicalTypes[fields[i]], fields[i]);
        }

        return row -> {
            GenericRowData rowData = new GenericRowData(row.getRowKind(), fieldGetters.length);
            for (int i = 0; i < fieldGetters.length; i++) {
                rowData.setField(i, fieldGetters[i].getFieldOrNull(row));
            }
            return rowData;
        };
    }
}
