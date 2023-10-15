package org.apache.flink.connector.clickhouse.internal.partitioner;

import org.apache.flink.connector.clickhouse.internal.schema.ClusterSpec;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.RowData.FieldGetter;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.nonNull;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Same as ClickHouse's hash function `javaHash`. <br>
 * ! Extended to integers from ClickHouse release 22.10.
 */
public class JavaHashPartitioner extends ClickHousePartitioner {

    private final FieldGetter fieldGetter;

    public JavaHashPartitioner(List<FieldGetter> getters) {
        checkArgument(
                getters.size() == 1 && nonNull(getters.get(0)),
                "The parameter number of JavaHashPartitioner must be 1");
        this.fieldGetter = getters.get(0);
    }

    @Override
    public int select(RowData record, ClusterSpec clusterSpec) {
        long weightSum = clusterSpec.getWeightSum();
        long result = Objects.hashCode(fieldGetter.getFieldOrNull(record)) % weightSum;
        return select(result, clusterSpec);
    }
}
