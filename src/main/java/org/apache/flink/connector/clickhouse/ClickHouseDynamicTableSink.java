//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.flink.connector.clickhouse;

import org.apache.flink.connector.clickhouse.internal.AbstractClickHouseOutputFormat;
import org.apache.flink.connector.clickhouse.internal.options.ClickHouseOptions;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.OutputFormatProvider;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A {@link DynamicTableSink} that describes how to create a {@link ClickHouseDynamicTableSink} from
 * a logical description.
 *
 * <p>TODO: Partitioning strategy isn't well implemented.
 */
public class ClickHouseDynamicTableSink implements DynamicTableSink, SupportsPartitioning {

    private final CatalogTable catalogTable;

    private final ResolvedSchema tableSchema;

    private final ClickHouseOptions options;

    private boolean dynamicGrouping = false;

    private LinkedHashMap<String, String> staticPartitionSpec = new LinkedHashMap<>();

    public ClickHouseDynamicTableSink(
            ClickHouseOptions options, CatalogTable catalogTable, ResolvedSchema tableSchema) {
        this.options = options;
        this.catalogTable = catalogTable;
        this.tableSchema = tableSchema;
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
                        .withFieldNames(tableSchema.getColumnNames().toArray(new String[0]))
                        .withFieldDataTypes(
                                tableSchema.getColumnDataTypes().toArray(new DataType[0]))
                        .withPrimaryKey(tableSchema.getPrimaryKey().orElse(null))
                        .withPartitionKey(catalogTable.getPartitionKeys())
                        .build();
        return OutputFormatProvider.of(outputFormat);
    }

    @Override
    public void applyStaticPartition(Map<String, String> partition) {
        staticPartitionSpec = new LinkedHashMap<>();
        for (String partitionCol : catalogTable.getPartitionKeys()) {
            if (partition.containsKey(partitionCol)) {
                staticPartitionSpec.put(partitionCol, partition.get(partitionCol));
            }
        }
    }

    @Override
    public boolean requiresPartitionGrouping(boolean supportsGrouping) {
        this.dynamicGrouping = supportsGrouping;
        return supportsGrouping;
    }

    @Override
    public DynamicTableSink copy() {
        ClickHouseDynamicTableSink sink =
                new ClickHouseDynamicTableSink(options, catalogTable, tableSchema);
        sink.dynamicGrouping = dynamicGrouping;
        sink.staticPartitionSpec = staticPartitionSpec;
        return sink;
    }

    @Override
    public String asSummaryString() {
        return "ClickHouse table sink";
    }
}
