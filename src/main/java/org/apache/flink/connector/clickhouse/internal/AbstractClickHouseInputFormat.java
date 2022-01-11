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

    @Override
    public void configure(Configuration parameters) {}

    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) {
        return cachedStatistics;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return null;
    }

    /** Builder. */
    public static class Builder {

        private ClickHouseReadOptions options;

        private Properties connectionProperties;

        private String[] fieldNames;

        private DataType[] fieldTypes;

        private Object[][] parameterValues;

        private String parameterClause;

        public Builder withOptions(ClickHouseReadOptions options) {
            this.options = options;
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

        public Builder withSplitParametersProvider(
                ClickHouseParametersProvider parameterValuesProvider) {
            this.parameterValues = parameterValuesProvider.getParameterValues();
            return this;
        }

        public Builder withSplitParametersClause(String parameterClause) {
            this.parameterClause = parameterClause;
            return this;
        }

        public AbstractClickHouseInputFormat build() {
            Preconditions.checkNotNull(options);
            Preconditions.checkNotNull(connectionProperties);
            Preconditions.checkNotNull(fieldNames);
            LogicalType[] logicalTypes =
                    Arrays.stream(fieldTypes)
                            .map(DataType::getLogicalType)
                            .toArray(LogicalType[]::new);

            return options.isUseLocal()
                    ? createShardInputFormat(logicalTypes)
                    : createBatchOutputFormat(logicalTypes);
        }

        private AbstractClickHouseInputFormat createShardInputFormat(LogicalType[] logicalTypes) {
            return new ClickHouseBatchInputFormat(
                    new ClickHouseConnectionProvider(options, connectionProperties),
                    new ClickHouseRowConverter(RowType.of(logicalTypes)),
                    fieldNames,
                    null,
                    null,
                    options);
        }

        private AbstractClickHouseInputFormat createBatchOutputFormat(LogicalType[] logicalTypes) {
            return new ClickHouseBatchInputFormat(
                    new ClickHouseConnectionProvider(options, connectionProperties),
                    new ClickHouseRowConverter(RowType.of(logicalTypes)),
                    fieldNames,
                    parameterValues,
                    parameterClause,
                    options);
        }
    }
}
