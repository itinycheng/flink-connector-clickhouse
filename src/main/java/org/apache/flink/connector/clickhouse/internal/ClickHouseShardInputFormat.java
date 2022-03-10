package org.apache.flink.connector.clickhouse.internal;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.clickhouse.internal.common.DistributedEngineFullSchema;
import org.apache.flink.connector.clickhouse.internal.connection.ClickHouseConnectionProvider;
import org.apache.flink.connector.clickhouse.internal.converter.ClickHouseRowConverter;
import org.apache.flink.connector.clickhouse.internal.options.ClickHouseReadOptions;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.RowData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.ClickHouseConnection;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** ClickHouse shard input format. */
@Experimental
public class ClickHouseShardInputFormat extends AbstractClickHouseInputFormat {

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseShardInputFormat.class);

    private final ClickHouseConnectionProvider connectionProvider;

    private final ClickHouseRowConverter rowConverter;

    private final ClickHouseReadOptions readOptions;

    private final DistributedEngineFullSchema engineFullSchema;

    private final Map<Integer, String> shardMap;

    private final Object[][] shardValues;

    private transient List<PreparedStatement> statements;
    private transient List<ResultSet> resultSets;
    private transient boolean hasNext;
    private transient int rsIndex = -1;

    public ClickHouseShardInputFormat(
            ClickHouseConnectionProvider connectionProvider,
            ClickHouseRowConverter rowConverter,
            ClickHouseReadOptions readOptions,
            DistributedEngineFullSchema engineFullSchema,
            Map<Integer, String> shardMap,
            Object[][] shardValues,
            String[] fieldNames,
            TypeInformation<RowData> rowDataTypeInfo,
            Object[][] parameterValues,
            String parameterClause,
            String filterClause,
            long limit) {
        super(fieldNames, rowDataTypeInfo, parameterValues, parameterClause, filterClause, limit);
        this.connectionProvider = connectionProvider;
        this.rowConverter = rowConverter;
        this.readOptions = readOptions;
        this.engineFullSchema = engineFullSchema;
        this.shardMap = shardMap;
        this.shardValues = shardValues;
    }

    @Override
    public void configure(Configuration parameters) {
        super.configure(parameters);
        this.statements = new ArrayList<>();
        this.resultSets = new ArrayList<>();
    }

    @Override
    public void openInputFormat() {}

    @Override
    public void closeInputFormat() {
        if (connectionProvider != null) {
            connectionProvider.closeConnections();
        }
    }

    @Override
    public void open(InputSplit split) {
        try {
            Object[] shardIds = shardValues[split.getSplitNumber()];
            for (int i = 0; i < shardIds.length; i++) {
                // PreparedStatement.
                String shardUrl = shardMap.get((Integer) shardIds[i]);
                ClickHouseConnection connection =
                        connectionProvider.createAndStoreShardConnection(
                                shardUrl, engineFullSchema.getDatabase());
                String query =
                        getQuery(engineFullSchema.getTable(), engineFullSchema.getDatabase());
                PreparedStatement statement = connection.prepareStatement(query);
                statements.add(i, statement);

                // ResultSet.
                if (parameterValues != null) {
                    Object[] parameters = parameterValues[split.getSplitNumber()];
                    for (int j = 0; j < parameters.length; j++) {
                        statement.setObject(j + 1, parameters[j]);
                    }
                }

                if (i == 0) {
                    ResultSet resultSet = statement.executeQuery();
                    resultSets.add(i, resultSet);
                    hasNext = resultSet.next();
                }
            }

            rsIndex = 0;
        } catch (SQLException se) {
            throw new IllegalArgumentException("open() failed." + se.getMessage(), se);
        }
    }

    @Override
    public void close() {
        for (ResultSet resultSet : resultSets) {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
            } catch (SQLException se) {
                LOG.info("InputFormat ResultSet couldn't be closed.", se);
            }
        }

        for (PreparedStatement statement : statements) {
            try {
                if (statement != null) {
                    statement.close();
                }
            } catch (SQLException se) {
                LOG.info("InputFormat Statement couldn't be closed.", se);
            }
        }

        resultSets.clear();
        statements.clear();
    }

    @Override
    public boolean reachedEnd() {
        final int maxIndex = statements.size() - 1;
        return !hasNext && rsIndex == maxIndex;
    }

    @Override
    public RowData nextRecord(RowData reuse) throws IOException {
        if (!hasNext && !nextValidResultSet()) {
            return null;
        }

        try {
            ResultSet resultSet = resultSets.get(rsIndex);
            RowData row = rowConverter.toInternal(resultSet);
            // update hasNext after we've read the record
            hasNext = resultSet.next();
            return row;
        } catch (Exception exception) {
            throw new IOException("Couldn't read data from resultSet.", exception);
        }
    }

    private boolean nextValidResultSet() {
        while (++rsIndex < statements.size()) {
            try {
                PreparedStatement statement = statements.get(rsIndex);
                ResultSet resultSet = statement.executeQuery();
                resultSets.add(rsIndex, resultSet);
                hasNext = resultSet.next();

                if (hasNext) {
                    return true;
                }
            } catch (SQLException e) {
                throw new RuntimeException("Execute query failed, rsIndex = " + rsIndex);
            }
        }

        return false;
    }

    @Override
    public InputSplit[] createInputSplits(int minNumSplits) {
        return createGenericInputSplits(shardValues.length);
    }
}
