package org.apache.flink.connector.clickhouse.internal.partitioner;

import org.apache.flink.connector.clickhouse.internal.schema.ClusterSpec;
import org.apache.flink.connector.clickhouse.internal.schema.ShardSpec;
import org.apache.flink.table.data.RowData;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/** Shuffle data by random numbers. */
public class ShufflePartitioner extends ClickHousePartitioner {

    public ShufflePartitioner() {}

    @Override
    public int select(RowData record, ClusterSpec clusterSpec) {
        List<ShardSpec> shards = clusterSpec.getShards();
        int index = ThreadLocalRandom.current().nextInt(shards.size());
        return shards.get(index).getNum();
    }
}
