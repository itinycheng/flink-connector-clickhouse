//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package com.tiny.flink.connector.clickhouse.internal.partitioner;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.RowData.FieldGetter;

import java.io.Serializable;

/**
 * @author tiger
 */
public interface ClickHousePartitioner extends Serializable {

    String BALANCED = "balanced";

    String SHUFFLE = "shuffle";

    String HASH = "hash";

    int select(RowData var1, int var2);

    static ClickHousePartitioner createBalanced() {
        return new BalancedPartitioner();
    }

    static ClickHousePartitioner createShuffle() {
        return new ShufflePartitioner();
    }

    static ClickHousePartitioner createHash(FieldGetter getter) {
        return new HashPartitioner(getter);
    }
}
