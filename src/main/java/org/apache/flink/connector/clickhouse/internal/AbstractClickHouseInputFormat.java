package org.apache.flink.connector.clickhouse.internal;

import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.clickhouse.internal.common.DistributedEngineFullSchema;
import org.apache.flink.connector.clickhouse.internal.connection.ClickHouseConnectionProvider;
import org.apache.flink.connector.clickhouse.internal.converter.ClickHouseRowConverter;
import org.apache.flink.connector.clickhouse.internal.options.ClickHouseReadOptions;
import org.apache.flink.connector.clickhouse.split.ClickHouseParametersProvider;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.connector.clickhouse.util.ClickHouseUtil.getAndParseDistributedEngineSchema;

/** Abstract Clickhouse input format. */
public abstract class AbstractClickHouseInputFormat extends RichInputFormat<RowData, InputSplit>
        implements ResultTypeQueryable<RowData> {

    protected final String[] fieldNames;

    protected final TypeInformation<RowData> rowDataTypeInfo;

    protected final Object[][] parameterValues;

    protected final String parameterClause;

    protected final String filterClause;

    protected final long limit;

    protected AbstractClickHouseInputFormat(
            String[] fieldNames,
            TypeInformation<RowData> rowDataTypeInfo,
            Object[][] parameterValues,
            String parameterClause,
            String filterClause,
            long limit) {
        this.fieldNames = fieldNames;
        this.rowDataTypeInfo = rowDataTypeInfo;
        this.parameterValues = parameterValues;
        this.parameterClause = parameterClause;
        this.filterClause = filterClause;
        this.limit = limit;
    }

    @Override
    public void configure(Configuration parameters) {}

    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) {
        return cachedStatistics;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return rowDataTypeInfo;
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
        return new DefaultInputSplitAssigner(inputSplits);
    }

    protected InputSplit[] createGenericInputSplits(int splitNum) {
        GenericInputSplit[] ret = new GenericInputSplit[splitNum];
        for (int i = 0; i < ret.length; i++) {
            ret[i] = new GenericInputSplit(i, ret.length);
        }
        return ret;
    }

    protected String getQuery(String table, String database) {
        String queryTemplate =
                ClickHouseStatementFactory.getSelectStatement(table, database, fieldNames);
        StringBuilder whereBuilder = new StringBuilder();
        if (filterClause != null) {
            if (filterClause.toLowerCase().contains(" or ")) {
                whereBuilder.append("(").append(filterClause).append(")");
            } else {
                whereBuilder.append(filterClause);
            }
        }

        if (parameterClause != null) {
            if (whereBuilder.length() > 0) {
                whereBuilder.append(" AND ");
            }
            whereBuilder.append(parameterClause);
        }

        String limitClause = "";
        if (limit >= 0) {
            limitClause = "LIMIT " + limit;
        }

        return whereBuilder.length() > 0
                ? String.join(" ", queryTemplate, "WHERE", whereBuilder.toString(), limitClause)
                : String.join(" ", queryTemplate, limitClause);
    }

    /** Builder. */
    public static class Builder {

        private ClickHouseReadOptions readOptions;

        private Properties connectionProperties;

        private DistributedEngineFullSchema engineFullSchema;

        private Map<Integer, String> shardMap;

        private Object[][] shardValues;

        private String[] fieldNames;

        private DataType[] fieldTypes;

        private TypeInformation<RowData> rowDataTypeInfo;

        private Object[][] parameterValues;

        private String parameterClause;

        private String filterClause;

        private long limit;

        public Builder withOptions(ClickHouseReadOptions readOptions) {
            this.readOptions = readOptions;
            return this;
        }

        public Builder withConnectionProperties(Properties connectionProperties) {
            this.connectionProperties = connectionProperties;
            return this;
        }

        public Builder withFieldNames(String[] fieldNames) {
            this.fieldNames = fieldNames;
            return this;
        }

        public Builder withFieldTypes(DataType[] fieldTypes) {
            this.fieldTypes = fieldTypes;
            return this;
        }

        public Builder withRowDataTypeInfo(TypeInformation<RowData> rowDataTypeInfo) {
            this.rowDataTypeInfo = rowDataTypeInfo;
            return this;
        }

        public Builder withFilterClause(String filterClause) {
            this.filterClause = filterClause;
            return this;
        }

        public Builder withLimit(long limit) {
            this.limit = limit;
            return this;
        }

        public AbstractClickHouseInputFormat build() {
            Preconditions.checkNotNull(readOptions);
            Preconditions.checkNotNull(connectionProperties);
            Preconditions.checkNotNull(fieldNames);
            Preconditions.checkNotNull(fieldTypes);
            Preconditions.checkNotNull(rowDataTypeInfo);

            int[] shardIds = null;
            if (readOptions.isUseLocal()) {
                shardIds = initShardInfo();
            }
            if (readOptions.isUseLocal() || readOptions.getPartitionColumn() != null) {
                initPartitionInfo(shardIds);
            }

            LogicalType[] logicalTypes =
                    Arrays.stream(fieldTypes)
                            .map(DataType::getLogicalType)
                            .toArray(LogicalType[]::new);
            return readOptions.isUseLocal()
                    ? createShardInputFormat(logicalTypes)
                    : createBatchOutputFormat(logicalTypes);
        }

        private int[] initShardInfo() {
            ClickHouseConnectionProvider connectionProvider = null;
            try {
                connectionProvider =
                        new ClickHouseConnectionProvider(readOptions, connectionProperties);
                engineFullSchema =
                        getAndParseDistributedEngineSchema(
                                connectionProvider.getOrCreateConnection(),
                                readOptions.getDatabaseName(),
                                readOptions.getTableName());

                if (engineFullSchema == null) {
                    throw new RuntimeException(
                            String.format(
                                    "table `%s`.`%s` is not a Distributed table",
                                    readOptions.getDatabaseName(), readOptions.getTableName()));
                }

                List<String> shardUrls =
                        connectionProvider.getShardUrls(engineFullSchema.getCluster());
                if (!shardUrls.isEmpty()) {
                    int len = shardUrls.size();
                    int[] dataIds = new int[len];
                    shardMap = new HashMap<>(len);
                    for (int i = 0; i < len; i++) {
                        shardMap.put(i, shardUrls.get(i));
                        dataIds[i] = i;
                    }
                    return dataIds;
                }
            } catch (Exception exception) {
                throw new RuntimeException("Get shard table info failed.", exception);
            } finally {
                if (connectionProvider != null) {
                    connectionProvider.closeConnections();
                }
            }

            return null;
        }

        private void initPartitionInfo(int[] shardIds) {
            try {
                ClickHouseParametersProvider parametersProvider =
                        new ClickHouseParametersProvider.Builder()
                                .setMinVal(readOptions.getPartitionLowerBound())
                                .setMaxVal(readOptions.getPartitionUpperBound())
                                .setBatchNum(readOptions.getPartitionNum())
                                .setUseLocal(readOptions.isUseLocal())
                                .setShardIds(shardIds)
                                .build();

                this.parameterValues = parametersProvider.getParameterValues();
                String parameterClause = parametersProvider.getParameterClause();
                if (parameterClause != null) {
                    this.parameterClause =
                            String.format(
                                    parametersProvider.getParameterClause(),
                                    readOptions.getPartitionColumn());
                }
                this.shardValues = parametersProvider.getShardIdValues();
            } catch (Exception exception) {
                throw new RuntimeException("Init partition failed.", exception);
            }
        }

        private AbstractClickHouseInputFormat createShardInputFormat(LogicalType[] logicalTypes) {
            return new ClickHouseShardInputFormat(
                    new ClickHouseConnectionProvider(readOptions, connectionProperties),
                    new ClickHouseRowConverter(RowType.of(logicalTypes)),
                    readOptions,
                    engineFullSchema,
                    shardMap,
                    shardValues,
                    fieldNames,
                    rowDataTypeInfo,
                    parameterValues,
                    parameterClause,
                    filterClause,
                    limit);
        }

        private AbstractClickHouseInputFormat createBatchOutputFormat(LogicalType[] logicalTypes) {
            return new ClickHouseBatchInputFormat(
                    new ClickHouseConnectionProvider(readOptions, connectionProperties),
                    new ClickHouseRowConverter(RowType.of(logicalTypes)),
                    readOptions,
                    fieldNames,
                    rowDataTypeInfo,
                    parameterValues,
                    parameterClause,
                    filterClause,
                    limit);
        }
    }
}
