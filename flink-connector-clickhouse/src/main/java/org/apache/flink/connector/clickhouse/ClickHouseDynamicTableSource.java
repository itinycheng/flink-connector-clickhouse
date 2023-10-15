package org.apache.flink.connector.clickhouse;

import org.apache.flink.connector.clickhouse.internal.AbstractClickHouseInputFormat;
import org.apache.flink.connector.clickhouse.internal.ClickHouseRowDataLookupFunction;
import org.apache.flink.connector.clickhouse.internal.options.ClickHouseReadOptions;
import org.apache.flink.connector.clickhouse.util.FilterPushDownHelper;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.connector.source.lookup.LookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.PartialCachingLookupProvider;
import org.apache.flink.table.connector.source.lookup.cache.LookupCache;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/** ClickHouse table source. */
public class ClickHouseDynamicTableSource
        implements ScanTableSource,
                LookupTableSource,
                SupportsProjectionPushDown,
                SupportsLimitPushDown,
                SupportsFilterPushDown {

    private final ClickHouseReadOptions readOptions;

    private final Properties connectionProperties;

    private final int lookupMaxRetryTimes;

    @Nullable private final LookupCache cache;

    private DataType physicalRowDataType;

    private String filterClause;

    private long limit = -1L;

    public ClickHouseDynamicTableSource(
            ClickHouseReadOptions readOptions,
            int lookupMaxRetryTimes,
            @Nullable LookupCache cache,
            Properties properties,
            DataType physicalRowDataType) {
        this.readOptions = readOptions;
        this.connectionProperties = properties;
        this.lookupMaxRetryTimes = lookupMaxRetryTimes;
        this.cache = cache;
        this.physicalRowDataType = physicalRowDataType;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        // ClickHouse only support non-nested look up keys
        String[] keyNames = new String[context.getKeys().length];
        for (int i = 0; i < keyNames.length; i++) {
            int[] innerKeyArr = context.getKeys()[i];
            Preconditions.checkArgument(
                    innerKeyArr.length == 1, "ClickHouse only support non-nested look up keys");
            keyNames[i] = DataType.getFieldNames(physicalRowDataType).get(innerKeyArr[0]);
        }
        final RowType rowType = (RowType) physicalRowDataType.getLogicalType();
        ClickHouseRowDataLookupFunction lookupFunction =
                new ClickHouseRowDataLookupFunction(
                        readOptions,
                        lookupMaxRetryTimes,
                        DataType.getFieldNames(physicalRowDataType).toArray(new String[0]),
                        DataType.getFieldDataTypes(physicalRowDataType).toArray(new DataType[0]),
                        keyNames,
                        rowType);
        if (cache != null) {
            return PartialCachingLookupProvider.of(lookupFunction, cache);
        } else {
            return LookupFunctionProvider.of(lookupFunction);
        }
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        AbstractClickHouseInputFormat.Builder builder =
                new AbstractClickHouseInputFormat.Builder()
                        .withOptions(readOptions)
                        .withConnectionProperties(connectionProperties)
                        .withFieldNames(
                                DataType.getFieldNames(physicalRowDataType).toArray(new String[0]))
                        .withFieldTypes(
                                DataType.getFieldDataTypes(physicalRowDataType)
                                        .toArray(new DataType[0]))
                        .withRowDataTypeInfo(
                                runtimeProviderContext.createTypeInformation(physicalRowDataType))
                        .withFilterClause(filterClause)
                        .withLimit(limit);
        return InputFormatProvider.of(builder.build());
    }

    @Override
    public DynamicTableSource copy() {
        ClickHouseDynamicTableSource source =
                new ClickHouseDynamicTableSource(
                        readOptions,
                        lookupMaxRetryTimes,
                        cache,
                        connectionProperties,
                        physicalRowDataType);
        source.filterClause = filterClause;
        source.limit = limit;
        return source;
    }

    @Override
    public String asSummaryString() {
        return "ClickHouse table source";
    }

    @Override
    public Result applyFilters(List<ResolvedExpression> filters) {
        this.filterClause = FilterPushDownHelper.convert(filters);
        return Result.of(new ArrayList<>(filters), new ArrayList<>(filters));
    }

    @Override
    public void applyLimit(long limit) {
        this.limit = limit;
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields, DataType producedDataType) {
        this.physicalRowDataType = Projection.of(projectedFields).project(physicalRowDataType);
    }
}
