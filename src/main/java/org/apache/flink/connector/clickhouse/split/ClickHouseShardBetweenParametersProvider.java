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
