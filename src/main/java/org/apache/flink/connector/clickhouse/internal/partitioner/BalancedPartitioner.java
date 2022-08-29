package org.apache.flink.connector.clickhouse.internal.partitioner;

import org.apache.flink.table.data.RowData;

/** Use round-robin mode to partition data. */
public class BalancedPartitioner extends ClickHousePartitioner {

    private static final long serialVersionUID = 1L;

    private int nextShard = 0;

    public BalancedPartitioner() {
        super(null);
    }

    @Override
    public int select(RowData record, int numShards) {
        nextShard = (nextShard + 1) % numShards;
        return nextShard;
    }
}
