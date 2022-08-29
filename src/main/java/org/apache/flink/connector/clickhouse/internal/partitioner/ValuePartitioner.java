package org.apache.flink.connector.clickhouse.internal.partitioner;

import org.apache.flink.table.data.RowData;

import java.math.BigDecimal;
import java.math.BigInteger;

/** Partition key value based, value must be a number. */
public class ValuePartitioner extends ClickHousePartitioner {

    public ValuePartitioner(RowData.FieldGetter fieldGetter) {
        super(fieldGetter);
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
        } else if (value instanceof BigDecimal) {
            num = ((BigDecimal) value).toBigInteger().mod(BigInteger.valueOf(numShards)).intValue();
        } else if (value instanceof BigInteger) {
            num = ((BigInteger) value).mod(BigInteger.valueOf(numShards)).intValue();
        } else {
            Class<?> valueClass = value == null ? null : value.getClass();
            throw new RuntimeException("Unsupported number type: " + valueClass);
        }

        return Math.abs(num);
    }
}
