package org.apache.flink.connector.clickhouse.internal.partitioner;

import org.apache.flink.connector.clickhouse.internal.schema.ClusterSpec;
import org.apache.flink.connector.clickhouse.internal.schema.ShardSpec;
import org.apache.flink.table.data.RowData;

import java.io.Serializable;

/** ClickHouse data partitioner interface. */
public abstract class ClickHousePartitioner implements Serializable {

    private static final long serialVersionUID = 1L;

    public abstract int select(RowData record, ClusterSpec clusterSpec);

    public int select(long value, ClusterSpec clusterSpec) {
        value = Math.abs(value);
        for (ShardSpec shard : clusterSpec.getShards()) {
            if (shard.isInShardRangeBounds(value)) {
                return shard.getNum();
            }
        }

        throw new IllegalStateException(
                String.format(
                        "Unreachable, partitioner: %s must has some kind of bug",
                        this.getClass().getName()));
    }
}
