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

package org.apache.flink.connector.clickhouse.split;

import org.apache.flink.annotation.Experimental;

import org.apache.commons.lang3.ArrayUtils;

import java.util.Arrays;

import static org.apache.flink.util.Preconditions.checkArgument;

/** For example, $columnName BETWEEN ? AND ? */
@Experimental
public class ClickHouseShardTableParametersProvider extends ClickHouseParametersProvider {

    private final int[] shardIds;
    private final int shardNum;

    public ClickHouseShardTableParametersProvider(int[] shardIds) {
        checkArgument(shardIds.length > 0, "length of shardIds must be larger than 0");
        this.shardIds = shardIds;
        this.shardNum = shardIds.length;
    }

    @Override
    public String getParameterClause() {
        return null;
    }

    @Override
    public ClickHouseShardTableParametersProvider ofBatchNum(Integer batchNum) {
        batchNum = batchNum != null ? batchNum : shardNum;
        checkArgument(batchNum > 0, "batchNum must be positive");

        if (batchNum > shardNum) {
            batchNum = shardNum;
        }
        this.batchNum = batchNum;
        return this;
    }

    @Override
    public ClickHouseShardTableParametersProvider calculate() {
        int minBatchSize = shardNum / batchNum;
        int minBatchNum = (minBatchSize + 1) * batchNum - shardNum;
        int[] info = allocateShards(minBatchSize, minBatchNum, batchNum);

        Integer[][] shardIdValues = null;
        for (int i = 0; i < info.length; i++) {
            int start = Arrays.stream(ArrayUtils.subarray(info, 0, i)).sum();
            shardIdValues = ArrayUtils.add(shardIdValues, subShardIds(start, info[i], shardIds));
        }

        this.shardIdValues = shardIdValues;
        return this;
    }
}
