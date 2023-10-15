package org.apache.flink.connector.clickhouse.internal.partitioner;

import org.apache.flink.connector.clickhouse.internal.schema.ClusterSpec;
import org.apache.flink.connector.clickhouse.internal.schema.ShardSpec;
import org.apache.flink.table.data.RowData;

import java.util.List;

/** Use round-robin mode to partition data. */
public class BalancedPartitioner extends ClickHousePartitioner {

    private static final long serialVersionUID = 1L;

    private int nextShard = 0;

    public BalancedPartitioner() {}

    @Override
    public int select(RowData record, ClusterSpec clusterSpec) {
        List<ShardSpec> shards = clusterSpec.getShards();
        nextShard = (nextShard + 1) % shards.size();
        return shards.get(nextShard).getNum();
    }
}
