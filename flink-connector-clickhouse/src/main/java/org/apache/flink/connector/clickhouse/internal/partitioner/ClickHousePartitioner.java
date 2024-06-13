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
import org.apache.flink.connector.clickhouse.internal.schema.ShardSpec;
import org.apache.flink.table.data.RowData;

import java.io.Serializable;

/** ClickHouse data partitioner interface. */
public abstract class ClickHousePartitioner implements Serializable {

    private static final long serialVersionUID = 1L;

    public abstract int select(RowData record, ClusterSpec clusterSpec);

    public int select(long value, ClusterSpec clusterSpec) {
        value = Math.abs(value);
        for (ShardSpec shard : clusterSpec.getShards()) {
            if (shard.isInShardRangeBounds(value)) {
                return shard.getNum();
            }
        }

        throw new IllegalStateException(
                String.format(
                        "Unreachable, partitioner: %s must has some kind of bug",
                        this.getClass().getName()));
    }
}
