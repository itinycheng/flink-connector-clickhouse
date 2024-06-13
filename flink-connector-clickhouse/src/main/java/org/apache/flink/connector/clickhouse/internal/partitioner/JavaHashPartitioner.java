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
