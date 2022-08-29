package org.apache.flink.connector.clickhouse.internal.partitioner;

import org.apache.flink.table.data.RowData;

import java.util.concurrent.ThreadLocalRandom;

/** Shuffle data by random numbers. */
public class ShufflePartitioner extends ClickHousePartitioner {

    private static final long serialVersionUID = 1L;

    public ShufflePartitioner() {
        super(null);
    }

    @Override
    public int select(RowData record, int numShards) {
        return ThreadLocalRandom.current().nextInt(numShards);
    }
}
