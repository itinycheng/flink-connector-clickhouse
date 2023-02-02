package org.apache.flink.connector.clickhouse.internal.partitioner;

import org.apache.flink.table.data.RowData;

import java.io.Serializable;

/** ClickHouse data partitioner interface. */
public abstract class ClickHousePartitioner implements Serializable {

    private static final long serialVersionUID = 1L;

    public abstract int select(RowData record, int numShards);
}
