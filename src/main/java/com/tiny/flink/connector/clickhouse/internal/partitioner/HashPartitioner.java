//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package com.tiny.flink.connector.clickhouse.internal.partitioner;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.RowData.FieldGetter;

import java.util.Objects;

/** Use primary-key's hash code to partition data. */
public class HashPartitioner implements ClickHousePartitioner {

    private static final long serialVersionUID = 1L;

    private final FieldGetter getter;

    public HashPartitioner(FieldGetter getter) {
        this.getter = getter;
    }

    @Override
    public int select(RowData record, int numShards) {
        return Objects.hashCode(getter.getFieldOrNull(record)) % numShards;
    }
}
