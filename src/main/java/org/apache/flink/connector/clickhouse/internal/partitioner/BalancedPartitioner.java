//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.flink.connector.clickhouse.internal.partitioner;

import org.apache.flink.table.data.RowData;

/** Use round-robin mode to partition data. */
public class BalancedPartitioner implements ClickHousePartitioner {

    private static final long serialVersionUID = 1L;

    private int nextShard = 0;

    public BalancedPartitioner() {}

    @Override
    public int select(RowData record, int numShards) {
        nextShard = (nextShard + 1) % numShards;
        return nextShard;
    }
}
