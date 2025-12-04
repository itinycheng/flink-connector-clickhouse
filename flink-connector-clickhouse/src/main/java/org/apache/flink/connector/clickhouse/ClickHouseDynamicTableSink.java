/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.clickhouse;

import org.apache.flink.connector.clickhouse.internal.AbstractClickHouseOutputFormat;
import org.apache.flink.connector.clickhouse.internal.ClickHouseRowDataSink;
import org.apache.flink.connector.clickhouse.internal.options.ClickHouseDmlOptions;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/**
 * A {@link DynamicTableSink} that describes how to create a {@link ClickHouseDynamicTableSink} from
 * a logical description.
 *
 * <p>TODO: Partitioning strategy isn't well implemented.
 */
public class ClickHouseDynamicTableSink implements DynamicTableSink, SupportsPartitioning {

    private final String[] primaryKeys;

    private final String[] partitionKeys;

    private final DataType physicalRowDataType;

    private final ClickHouseDmlOptions options;

    private final Properties connectionProperties;

    private boolean dynamicGrouping = false;

    private LinkedHashMap<String, String> staticPartitionSpec = new LinkedHashMap<>();

    public ClickHouseDynamicTableSink(
            @Nonnull ClickHouseDmlOptions options,
            @Nonnull Properties connectionProperties,
            @Nonnull String[] primaryKeys,
            @Nonnull String[] partitionKeys,
            @Nonnull DataType physicalRowDataType) {
        this.options = options;
        this.connectionProperties = connectionProperties;
        this.primaryKeys = primaryKeys;
        this.partitionKeys = partitionKeys;
        this.physicalRowDataType = physicalRowDataType;
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
                ChangelogMode.insertOnly().equals(requestedMode) || primaryKeys.length > 0,
                "Please declare primary key for sink table when query contains update/delete record.");
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        AbstractClickHouseOutputFormat outputFormat =
                new AbstractClickHouseOutputFormat.Builder()
                        .withOptions(options)
                        .withConnectionProperties(connectionProperties)
                        .withFieldNames(
                                DataType.getFieldNames(physicalRowDataType).toArray(new String[0]))
                        .withFieldTypes(
                                DataType.getFieldDataTypes(physicalRowDataType)
                                        .toArray(new DataType[0]))
                        .withPrimaryKey(primaryKeys)
                        .withPartitionKey(partitionKeys)
                        .build();

        return SinkV2Provider.of(new ClickHouseRowDataSink(outputFormat));
    }

    @Override
    public void applyStaticPartition(Map<String, String> partition) {
        staticPartitionSpec = new LinkedHashMap<>();
        for (String partitionCol : partitionKeys) {
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
                new ClickHouseDynamicTableSink(
                        options,
                        connectionProperties,
                        primaryKeys,
                        partitionKeys,
                        physicalRowDataType);
        sink.dynamicGrouping = dynamicGrouping;
        sink.staticPartitionSpec = staticPartitionSpec;
        return sink;
    }

    @Override
    public String asSummaryString() {
        return "ClickHouse table sink";
    }
}
