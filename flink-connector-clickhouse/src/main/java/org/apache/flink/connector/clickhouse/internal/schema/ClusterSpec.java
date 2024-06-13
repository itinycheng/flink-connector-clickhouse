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

package org.apache.flink.connector.clickhouse.internal.schema;

import javax.annotation.Nonnull;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static java.util.stream.Collectors.toList;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Cluster. */
public class ClusterSpec implements Serializable {
    private final String name;

    private final List<ShardSpec> shards;

    private final Long weightSum;

    public ClusterSpec(@Nonnull String name, @Nonnull List<ShardSpec> shards) {
        this.name = checkNotNull(name);

        List<ShardSpec> sortedList = new ArrayList<>(shards).stream().sorted().collect(toList());
        this.shards = checkNotNull(sortedList);
        this.weightSum = sortedList.stream().mapToLong(ShardSpec::getWeight).sum();

        sortedList.forEach(
                shardSpec -> {
                    List<Long> weights =
                            sortedList.stream().map(ShardSpec::getWeight).collect(toList());
                    shardSpec.initShardRangeBound(weights);
                });
    }

    public String getName() {
        return name;
    }

    public List<ShardSpec> getShards() {
        return shards;
    }

    public Long getWeightSum() {
        return weightSum;
    }
}
