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

import java.io.Serializable;
import java.util.Arrays;

import static org.apache.flink.util.Preconditions.checkArgument;

/** For example, $columnName BETWEEN ? AND ? */
@Experimental
public class ClickHouseShardBetweenParametersProvider extends ClickHouseBetweenParametersProvider {

    private final int[] shardIds;
    private final int shardNum;

    public ClickHouseShardBetweenParametersProvider(long minVal, long maxVal, int[] shardIds) {
        super(minVal, maxVal);

        checkArgument(shardIds.length > 1, "length of shardIds must be larger than 0");
        this.shardIds = shardIds;
        this.shardNum = shardIds.length;
    }

    @Override
    public ClickHouseShardBetweenParametersProvider ofBatchNum(Integer batchNum) {
        checkArgument(batchNum != null && batchNum > 0, "batchNum must be positive");

        long maxElemCount = Math.max(maxVal - minVal, 1) * shardNum + 1;
        maxElemCount = defaultMaxIfLTZero(maxElemCount);
        if (batchNum > maxElemCount) {
            batchNum = (int) maxElemCount;
        }
        this.batchNum = batchNum;
        return this;
    }

    @Override
    public ClickHouseShardBetweenParametersProvider calculate() {
        Serializable[][] parameters = null;
        Integer[][] shardIdValues = null;

        float factor = ((float) batchNum) / shardNum;
        if (factor >= 1) {
            // e.g. batchNum = 10, shardNum = 3.
            int minBatchSize = (int) factor;
            int minBatchNum = (minBatchSize + 1) * shardNum - batchNum;
            int[] info = allocateShards(minBatchSize, minBatchNum, shardNum);
            for (int i = 0; i < info.length; i++) {
                parameters = ArrayUtils.addAll(parameters, divideParameterValues(info[i]));
                shardIdValues =
                        ArrayUtils.addAll(shardIdValues, repeatShardId(shardIds[i], info[i]));
            }
        } else if (factor < 1) {
            // e.g. batchNum = 10, shardNum = 23.
            int minBatchSize = (int) (1 / factor);
            int minBatchNum = (minBatchSize + 1) * batchNum - shardNum;
            int[] info = allocateShards(minBatchSize, minBatchNum, batchNum);
            for (int i = 0; i < info.length; i++) {
                int start = Arrays.stream(ArrayUtils.subarray(info, 0, i)).sum();
                parameters = ArrayUtils.addAll(parameters, divideParameterValues(1));
                shardIdValues =
                        ArrayUtils.add(shardIdValues, subShardIds(start, info[i], shardIds));
            }
        }

        this.parameterValues = parameters;
        this.shardIdValues = shardIdValues;
        return this;
    }

    private Integer[][] repeatShardId(int shardId, int shardNum) {
        Integer[][] shards = new Integer[shardNum][1];
        for (int i = 0; i < shardNum; i++) {
            shards[i] = new Integer[] {shardId};
        }
        return shards;
    }
}
