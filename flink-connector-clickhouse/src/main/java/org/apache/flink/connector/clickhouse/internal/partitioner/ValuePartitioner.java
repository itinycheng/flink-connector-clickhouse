/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.clickhouse.internal.partitioner;

import org.apache.flink.connector.clickhouse.internal.schema.ClusterSpec;
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
    public int select(RowData record, ClusterSpec clusterSpec) {
        Object value = fieldGetter.getFieldOrNull(record);
        long weightSum = clusterSpec.getWeightSum();

        long num;
        if (value instanceof Byte) {
            num = (byte) value % weightSum;
        } else if (value instanceof Short) {
            num = (short) value % weightSum;
        } else if (value instanceof Integer) {
            num = (int) value % weightSum;
        } else if (value instanceof Long) {
            num = (int) ((long) value % weightSum);
        } else if (value instanceof Float) {
            num = (int) ((float) value % weightSum);
        } else if (value instanceof Double) {
            num = (int) ((double) value % weightSum);
        } else if (value instanceof DecimalData) {
            num =
                    ((DecimalData) value)
                            .toBigDecimal()
                            .toBigInteger()
                            .mod(BigInteger.valueOf(weightSum))
                            .intValue();
        } else {
            Class<?> valueClass = value == null ? null : value.getClass();
            throw new RuntimeException("Unsupported number type: " + valueClass);
        }

        return select(num, clusterSpec);
    }
}
