//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package com.tiny.flink.connector.clickhouse;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.OutputFormatProvider;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import com.tiny.flink.connector.clickhouse.internal.AbstractClickHouseOutputFormat;
import com.tiny.flink.connector.clickhouse.internal.options.ClickHouseOptions;

/**
 * A {@link DynamicTableSink} that describes how to create a {@link ClickHouseDynamicTableSink} from
 * a logical description.
 */
public class ClickHouseDynamicTableSink implements DynamicTableSink {

    private final TableSchema tableSchema;

    private final ClickHouseOptions options;

    public ClickHouseDynamicTableSink(ClickHouseOptions options, TableSchema tableSchema) {
        this.options = options;
        this.tableSchema = tableSchema;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        this.validatePrimaryKey(requestedMode);
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .addContainedKind(RowKind.DELETE)
                .build();
    }

    private void validatePrimaryKey(ChangelogMode requestedMode) {
        Preconditions.checkState(
                ChangelogMode.insertOnly().equals(requestedMode)
                        || this.tableSchema.getPrimaryKey().isPresent(),
                "please declare primary key for sink table when query contains update/delete record.");
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        AbstractClickHouseOutputFormat outputFormat =
                new AbstractClickHouseOutputFormat.Builder()
                        .withOptions(this.options)
                        .withFieldNames(this.tableSchema.getFieldNames())
                        .withFieldDataTypes(this.tableSchema.getFieldDataTypes())
                        .withPrimaryKey(this.tableSchema.getPrimaryKey().orElse(null))
                        .withRowDataTypeInfo(
                                context.createTypeInformation(this.tableSchema.toRowDataType()))
                        .build();
        return OutputFormatProvider.of(outputFormat);
    }

    @Override
    public DynamicTableSink copy() {
        return new ClickHouseDynamicTableSink(this.options, this.tableSchema);
    }

    @Override
    public String asSummaryString() {
        return "ClickHouse";
    }
}
