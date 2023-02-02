package org.apache.flink.connector.clickhouse.internal.partitioner;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;

import java.math.BigInteger;
import java.util.List;

import static java.util.Objects.nonNull;
import static org.apache.flink.util.Preconditions.checkArgument;

/** Partition key value based, value must be a number. */
public class ValuePartitioner extends ClickHousePartitioner {

    private final RowData.FieldGetter fieldGetter;

    public ValuePartitioner(List<RowData.FieldGetter> getters) {
        checkArgument(
                getters.size() == 1 && nonNull(getters.get(0)),
                "The parameter number of ValuePartitioner must be 1");
        this.fieldGetter = getters.get(0);
    }

    @Override
    public int select(RowData record, int numShards) {
        Object value = fieldGetter.getFieldOrNull(record);

        int num;
        if (value instanceof Byte) {
            num = (byte) value % numShards;
        } else if (value instanceof Short) {
            num = (short) value % numShards;
        } else if (value instanceof Integer) {
            num = (int) value % numShards;
        } else if (value instanceof Long) {
            num = (int) ((long) value % numShards);
        } else if (value instanceof Float) {
            num = (int) ((float) value % numShards);
        } else if (value instanceof Double) {
            num = (int) ((double) value % numShards);
        } else if (value instanceof DecimalData) {
            num =
                    ((DecimalData) value)
                            .toBigDecimal()
                            .toBigInteger()
                            .mod(BigInteger.valueOf(numShards))
                            .intValue();
        } else {
            Class<?> valueClass = value == null ? null : value.getClass();
            throw new RuntimeException("Unsupported number type: " + valueClass);
        }

        return Math.abs(num);
    }
}
