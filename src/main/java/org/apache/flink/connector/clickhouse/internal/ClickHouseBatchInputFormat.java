//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.flink.connector.clickhouse.internal;

import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.connector.clickhouse.internal.connection.ClickHouseConnectionProvider;
import org.apache.flink.connector.clickhouse.internal.converter.ClickHouseRowConverter;
import org.apache.flink.connector.clickhouse.internal.options.ClickHouseReadOptions;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.table.data.RowData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.ClickHouseConnection;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/** Output data to ClickHouse local table. */
public class ClickHouseBatchInputFormat extends AbstractClickHouseInputFormat {

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseBatchOutputFormat.class);

    private final ClickHouseConnectionProvider connectionProvider;

    private final ClickHouseRowConverter rowConverter;

    private final String[] fieldNames;

    private final Object[][] parameterValues;

    private final String parameterClause;

    private final ClickHouseReadOptions options;

    private transient PreparedStatement statement;
    private transient ResultSet resultSet;
    private transient boolean hasNext;

    public ClickHouseBatchInputFormat(
            ClickHouseConnectionProvider connectionProvider,
            ClickHouseRowConverter rowConverter,
            String[] fieldNames,
            Object[][] parameterValues,
            String parameterClause,
            ClickHouseReadOptions options) {
        this.connectionProvider = connectionProvider;
        this.rowConverter = rowConverter;
        this.fieldNames = fieldNames;
        this.parameterValues = parameterValues;
        this.parameterClause = parameterClause;
        this.options = options;
    }

    @Override
    public void openInputFormat() {
        try {
            ClickHouseConnection connection = connectionProvider.getOrCreateConnection();
            statement = connection.prepareStatement(getQuery());
        } catch (SQLException se) {
            throw new IllegalArgumentException("open() failed." + se.getMessage(), se);
        }
    }

    private String getQuery() {
        String queryTemplate =
                ClickHouseStatementFactory.getSelectStatement(
                        options.getTableName(), options.getDatabaseName(), fieldNames);
        if (parameterClause != null) {
            queryTemplate += " WHERE " + parameterClause;
        }
        return queryTemplate;
    }

    @Override
    public void closeInputFormat() {
        try {
            if (statement != null) {
                statement.close();
            }
        } catch (SQLException exception) {
            LOG.info("InputFormat Statement couldn't be closed.", exception);
        } finally {
            statement = null;
        }

        if (connectionProvider != null) {
            connectionProvider.closeConnections();
        }
    }

    @Override
    public void open(InputSplit inputSplit) {
        try {
            if (inputSplit != null && parameterValues != null) {
                for (int i = 0; i < parameterValues[inputSplit.getSplitNumber()].length; i++) {
                    Object param = parameterValues[inputSplit.getSplitNumber()][i];
                    statement.setObject(i + 1, param);
                }
            }

            resultSet = statement.executeQuery();
            hasNext = resultSet.next();
        } catch (SQLException se) {
            throw new IllegalArgumentException("open() failed." + se.getMessage(), se);
        }
    }

    @Override
    public void close() {
        try {
            if (resultSet != null) {
                resultSet.close();
            }
        } catch (SQLException se) {
            LOG.info("InputFormat ResultSet couldn't be closed.", se);
        }
    }

    @Override
    public boolean reachedEnd() {
        return !hasNext;
    }

    @Override
    public RowData nextRecord(RowData reuse) throws IOException {
        if (!hasNext) {
            return null;
        }

        try {
            RowData row = rowConverter.toInternal(resultSet);
            // update hasNext after we've read the record
            hasNext = resultSet.next();
            return row;
        } catch (Exception exception) {
            throw new IOException("Couldn't read data from resultSet.", exception);
        }
    }

    @Override
    public InputSplit[] createInputSplits(int minNumSplits) {
        if (parameterValues == null) {
            return new GenericInputSplit[] {new GenericInputSplit(0, 1)};
        }

        GenericInputSplit[] ret = new GenericInputSplit[parameterValues.length];
        for (int i = 0; i < ret.length; i++) {
            ret[i] = new GenericInputSplit(i, ret.length);
        }
        return ret;
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
        return new DefaultInputSplitAssigner(inputSplits);
    }
}
