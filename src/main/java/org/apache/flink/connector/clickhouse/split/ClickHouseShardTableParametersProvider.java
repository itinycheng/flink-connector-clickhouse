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
        checkArgument(shardIds.length > 1, "length of shardIds must be larger than 0");
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
