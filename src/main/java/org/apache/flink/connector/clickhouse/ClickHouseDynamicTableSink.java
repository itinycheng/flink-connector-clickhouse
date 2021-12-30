//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.flink.connector.clickhouse;

import org.apache.flink.connector.clickhouse.internal.AbstractClickHouseOutputFormat;
import org.apache.flink.connector.clickhouse.internal.options.ClickHouseOptions;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.OutputFormatProvider;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

/**
 * A {@link DynamicTableSink} that describes how to create a {@link ClickHouseDynamicTableSink} from
 * a logical description.
 */
public class ClickHouseDynamicTableSink implements DynamicTableSink {

    private final CatalogTable catalogTable;

    private final TableSchema tableSchema;

    private final ClickHouseOptions options;

    public ClickHouseDynamicTableSink(ClickHouseOptions options, CatalogTable table) {
        this.options = options;
        this.catalogTable = table;
        this.tableSchema = TableSchemaUtils.getPhysicalSchema(table.getSchema());
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        validatePrimaryKey(requestedMode);
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .addContainedKind(RowKind.DELETE)
                .build();
    }

    private void validatePrimaryKey(ChangelogMode requestedMode) {
        Preconditions.checkState(
                ChangelogMode.insertOnly().equals(requestedMode)
                        || tableSchema.getPrimaryKey().isPresent(),
                "Please declare primary key for sink table when query contains update/delete record.");
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        AbstractClickHouseOutputFormat outputFormat =
                new AbstractClickHouseOutputFormat.Builder()
                        .withOptions(options)
                        .withFieldNames(tableSchema.getFieldNames())
                        .withFieldDataTypes(tableSchema.getFieldDataTypes())
                        .withPrimaryKey(tableSchema.getPrimaryKey().orElse(null))
                        .withPartitionKey(catalogTable.getPartitionKeys())
                        .build();
        return OutputFormatProvider.of(outputFormat);
    }

    @Override
    public DynamicTableSink copy() {
        return new ClickHouseDynamicTableSink(options, catalogTable);
    }

    @Override
    public String asSummaryString() {
        return "ClickHouse table sink";
    }
}
