package org.apache.flink.connector.clickhouse.internal.partitioner;

import org.apache.flink.connector.clickhouse.internal.schema.ClusterSpec;
import org.apache.flink.table.data.RowData;

/** Use round-robin mode to partition data. */
public class BalancedPartitioner extends ClickHousePartitioner {

    private static final long serialVersionUID = 1L;

    private int nextShard = 0;

    public BalancedPartitioner() {}

    @Override
    public int select(RowData record, ClusterSpec clusterSpec) {
        int numShards = clusterSpec.getShards().size();
        nextShard = (nextShard + 1) % numShards;
        return nextShard;
    }
}
