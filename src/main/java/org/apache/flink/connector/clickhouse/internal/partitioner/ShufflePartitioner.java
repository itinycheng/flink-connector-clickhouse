package org.apache.flink.connector.clickhouse.internal.partitioner;

import org.apache.flink.connector.clickhouse.internal.schema.ClusterSpec;
import org.apache.flink.table.data.RowData;

import java.util.concurrent.ThreadLocalRandom;

/** Shuffle data by random numbers. */
public class ShufflePartitioner extends ClickHousePartitioner {

    public ShufflePartitioner() {}

    @Override
    public int select(RowData record, ClusterSpec clusterSpec) {
        int numShards = clusterSpec.getShards().size();
        return ThreadLocalRandom.current().nextInt(numShards);
    }
}
