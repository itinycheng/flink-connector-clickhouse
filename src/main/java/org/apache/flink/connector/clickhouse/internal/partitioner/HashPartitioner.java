package org.apache.flink.connector.clickhouse.internal.partitioner;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.RowData.FieldGetter;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.nonNull;
import static org.apache.flink.util.Preconditions.checkArgument;

/** Use primary-key's hash code to partition data. */
public class HashPartitioner extends ClickHousePartitioner {

    private final FieldGetter fieldGetter;

    public HashPartitioner(List<FieldGetter> getters) {
        checkArgument(
                getters.size() == 1 && nonNull(getters.get(0)),
                "The parameter number of HashPartitioner must be 1");
        this.fieldGetter = getters.get(0);
    }

    @Override
    public int select(RowData record, int numShards) {
        return Math.abs(Objects.hashCode(fieldGetter.getFieldOrNull(record)) % numShards);
    }
}
