package org.apache.flink.connector.clickhouse.internal.partitioner;

import org.apache.flink.table.data.RowData;

import java.util.concurrent.ThreadLocalRandom;

/** Shuffle data by random numbers. */
public class ShufflePartitioner extends ClickHousePartitioner {

    public ShufflePartitioner() {}

    @Override
    public int select(RowData record, int numShards) {
        return ThreadLocalRandom.current().nextInt(numShards);
    }
}
