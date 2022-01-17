package org.apache.flink.connector.clickhouse.split;

import java.io.Serializable;

/** Clickhouse parameters provider. */
public abstract class ClickHouseParametersProvider {

    protected Serializable[][] parameterValues;
    protected Serializable[][] shardIdValues;
    protected int batchNum;

    /** Returns the necessary parameters array to use for query in parallel a table. */
    public Serializable[][] getParameterValues() {
        return parameterValues;
    }

    /** Returns the shard ids that the parameter values act on. */
    public Serializable[][] getShardIdValues() {
        return shardIdValues;
    }

    public abstract String getParameterClause();

    public abstract ClickHouseParametersProvider ofBatchNum(Integer batchNum);

    public abstract ClickHouseParametersProvider calculate();

    // -------------------------- Methods for local tables --------------------------

    protected int[] allocateShards(int minBatchSize, int minBatchNum, int length) {
        int[] shards = new int[length];
        for (int i = 0; i < length; i++) {
            if (i + 1 <= minBatchNum) {
                shards[i] = minBatchSize;
            } else {
                shards[i] = minBatchSize + 1;
            }
        }
        return shards;
    }

    protected Integer[] subShardIds(int start, int idNum, int[] shardIds) {
        Integer[] subIds = new Integer[idNum];
        for (int i = 0; i < subIds.length; i++) {
            subIds[i] = shardIds[start + i];
        }
        return subIds;
    }

    /** Builder. */
    public static class Builder {

        private Long minVal;

        private Long maxVal;

        private Integer batchNum;

        private int[] shardIds;

        private boolean useLocal;

        public Builder setMinVal(Long minVal) {
            this.minVal = minVal;
            return this;
        }

        public Builder setMaxVal(Long maxVal) {
            this.maxVal = maxVal;
            return this;
        }

        public Builder setBatchNum(Integer batchNum) {
            this.batchNum = batchNum;
            return this;
        }

        public Builder setShardIds(int[] shardIds) {
            this.shardIds = shardIds;
            return this;
        }

        public Builder setUseLocal(boolean useLocal) {
            this.useLocal = useLocal;
            return this;
        }

        public ClickHouseParametersProvider build() {
            ClickHouseParametersProvider parametersProvider = null;
            if (minVal == null || maxVal == null) {
                if (useLocal) {
                    parametersProvider = new ClickHouseShardTableParametersProvider(shardIds);
                } else {
                    throw new RuntimeException("No suitable ClickHouseParametersProvider found.");
                }
            }

            if (parametersProvider == null) {
                parametersProvider =
                        useLocal && shardIds != null
                                ? new ClickHouseShardBetweenParametersProvider(
                                        minVal, maxVal, shardIds)
                                : new ClickHouseBatchBetweenParametersProvider(minVal, maxVal);
            }

            return parametersProvider.ofBatchNum(batchNum).calculate();
        }
    }
}
