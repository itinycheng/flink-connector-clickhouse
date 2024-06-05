package org.apache.flink.connector.clickhouse.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.clickhouse.ClickHouseDynamicTableSource;
import org.apache.flink.connector.clickhouse.internal.connection.ClickHouseConnectionProvider;
import org.apache.flink.connector.clickhouse.internal.connection.ClickHouseStatementWrapper;
import org.apache.flink.connector.clickhouse.internal.converter.ClickHouseRowConverter;
import org.apache.flink.connector.clickhouse.internal.options.ClickHouseReadOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.LookupFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import com.clickhouse.jdbc.ClickHousePreparedStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** A lookup function for {@link ClickHouseDynamicTableSource}. */
@Internal
public class ClickHouseRowDataLookupFunction extends LookupFunction {

    private static final Logger LOG =
            LoggerFactory.getLogger(ClickHouseRowDataLookupFunction.class);

    private final String query;
    private final ClickHouseConnectionProvider connectionProvider;
    private final int maxRetryTimes;
    private final ClickHouseRowConverter clickhouseRowConverter;
    private final ClickHouseRowConverter lookupKeyRowConverter;

    private transient ClickHouseStatementWrapper statement;

    public ClickHouseRowDataLookupFunction(
            ClickHouseReadOptions options,
            int maxRetryTimes,
            String[] fieldNames,
            DataType[] fieldTypes,
            String[] keyNames,
            RowType rowType) {
        checkNotNull(options, "No ClickHouseOptions supplied.");
        checkNotNull(fieldNames, "No fieldNames supplied.");
        checkNotNull(fieldTypes, "No fieldTypes supplied.");
        checkNotNull(keyNames, "No keyNames supplied.");
        this.connectionProvider = new ClickHouseConnectionProvider(options);
        List<String> nameList = Arrays.asList(fieldNames);
        DataType[] keyTypes =
                Arrays.stream(keyNames)
                        .map(
                                s -> {
                                    checkArgument(
                                            nameList.contains(s),
                                            "keyName %s can't find in fieldNames %s.",
                                            s,
                                            nameList);
                                    return fieldTypes[nameList.indexOf(s)];
                                })
                        .toArray(DataType[]::new);
        this.maxRetryTimes = maxRetryTimes;
        this.query =
                ClickHouseStatementFactory.getSelectWhereStatement(
                        options.getTableName(), options.getDatabaseName(), fieldNames, keyNames);
        this.clickhouseRowConverter = new ClickHouseRowConverter(rowType);
        this.lookupKeyRowConverter =
                new ClickHouseRowConverter(
                        RowType.of(
                                Arrays.stream(keyTypes)
                                        .map(DataType::getLogicalType)
                                        .toArray(LogicalType[]::new)));
    }

    @Override
    public void open(FunctionContext context) {
        try {
            establishConnectionAndStatement();
        } catch (SQLException sqe) {
            throw new IllegalArgumentException("open() failed.", sqe);
        }
    }

    /**
     * This is a lookup method which is called by Flink framework in runtime.
     *
     * @param keyRow lookup keys
     */
    @Override
    public Collection<RowData> lookup(RowData keyRow) {
        for (int retry = 0; retry <= maxRetryTimes; retry++) {
            try {
                statement.clearParameters();
                lookupKeyRowConverter.toExternal(keyRow, statement);
                try (ResultSet resultSet = statement.executeQuery()) {
                    ArrayList<RowData> rows = new ArrayList<>();
                    while (resultSet.next()) {
                        RowData row = clickhouseRowConverter.toInternal(resultSet);
                        rows.add(row);
                    }
                    rows.trimToSize();
                    return rows;
                }
            } catch (SQLException e) {
                LOG.error(
                        String.format("ClickHouse executeBatch error, retry times = %d", retry), e);
                if (retry >= maxRetryTimes) {
                    throw new RuntimeException("Execution of ClickHouse statement failed.", e);
                }

                try {
                    if (!connectionProvider.isConnectionValid()) {
                        statement.close();
                        connectionProvider.closeConnections();
                        establishConnectionAndStatement();
                    }
                } catch (SQLException exception) {
                    LOG.error(
                            "ClickHouse connection is not valid, and reestablish connection failed",
                            exception);
                    throw new RuntimeException(
                            "Reestablish ClickHouse connection failed", exception);
                }

                try {
                    Thread.sleep(1000L * retry);
                } catch (InterruptedException e1) {
                    throw new RuntimeException(e1);
                }
            }
        }
        return Collections.emptyList();
    }

    private void establishConnectionAndStatement() throws SQLException {
        Connection dbConn = connectionProvider.getOrCreateConnection();
        statement =
                new ClickHouseStatementWrapper(
                        (ClickHousePreparedStatement) dbConn.prepareStatement(query));
    }

    @Override
    public void close() {
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                LOG.info("ClickHouse statement could not be closed: " + e.getMessage());
            } finally {
                statement = null;
            }
        }

        connectionProvider.closeConnections();
    }
}
