package org.apache.flink.connector.clickhouse.internal;

import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.clickhouse.internal.connection.ClickHouseConnectionProvider;
import org.apache.flink.connector.clickhouse.internal.converter.ClickHouseRowConverter;
import org.apache.flink.connector.clickhouse.internal.options.ClickHouseReadOptions;
import org.apache.flink.connector.clickhouse.split.ClickHouseParametersProvider;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;
import java.util.Properties;

/** Clickhouse batch input format. */
public abstract class AbstractClickHouseInputFormat extends RichInputFormat<RowData, InputSplit>
        implements ResultTypeQueryable<RowData> {

    private final TypeInformation<RowData> rowDataTypeInfo;

    protected AbstractClickHouseInputFormat(TypeInformation<RowData> rowDataTypeInfo) {
        this.rowDataTypeInfo = rowDataTypeInfo;
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

    /** Builder. */
    public static class Builder {

        private ClickHouseReadOptions readOptions;

        private Properties connectionProperties;

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

        public Builder withSplitParametersProvider(
                ClickHouseParametersProvider parameterValuesProvider) {
            this.parameterValues = parameterValuesProvider.getParameterValues();
            return this;
        }

        public Builder withSplitParametersClause(String parameterClause) {
            this.parameterClause = parameterClause;
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

            LogicalType[] logicalTypes =
                    Arrays.stream(fieldTypes)
                            .map(DataType::getLogicalType)
                            .toArray(LogicalType[]::new);
            return readOptions.isUseLocal()
                    ? createShardInputFormat(logicalTypes)
                    : createBatchOutputFormat(logicalTypes);
        }

        private AbstractClickHouseInputFormat createShardInputFormat(LogicalType[] logicalTypes) {
            return new ClickHouseBatchInputFormat(
                    new ClickHouseConnectionProvider(readOptions, connectionProperties),
                    new ClickHouseRowConverter(RowType.of(logicalTypes)),
                    fieldNames,
                    null,
                    null,
                    null,
                    null,
                    -1,
                    readOptions);
        }

        private AbstractClickHouseInputFormat createBatchOutputFormat(LogicalType[] logicalTypes) {
            return new ClickHouseBatchInputFormat(
                    new ClickHouseConnectionProvider(readOptions, connectionProperties),
                    new ClickHouseRowConverter(RowType.of(logicalTypes)),
                    fieldNames,
                    rowDataTypeInfo,
                    parameterValues,
                    parameterClause,
                    filterClause,
                    limit,
                    readOptions);
        }
    }
}
