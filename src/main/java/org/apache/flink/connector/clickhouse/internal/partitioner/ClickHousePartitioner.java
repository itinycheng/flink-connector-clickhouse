package org.apache.flink.connector.clickhouse.internal.partitioner;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.RowData.FieldGetter;

import java.io.Serializable;

/** ClickHouse data partitioner interface. */
public abstract class ClickHousePartitioner implements Serializable {

    protected final FieldGetter fieldGetter;

    public ClickHousePartitioner(FieldGetter fieldGetter) {
        this.fieldGetter = fieldGetter;
    }

    public abstract int select(RowData record, int numShards);
}
