package org.apache.flink.connector.clickhouse.split;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/** For example, $columnName BETWEEN ? AND ? */
public class ClickHouseBetweenParametersProvider implements ClickHouseParametersProvider {

    public static final String BETWEEN_CLAUSE = "`%s` BETWEEN ? AND ?";

    private final long minVal;
    private final long maxVal;

    private long batchSize;
    private int batchNum;

    public ClickHouseBetweenParametersProvider(long minVal, long maxVal) {
        checkArgument(minVal <= maxVal, "minVal must not be larger than maxVal");
        this.minVal = minVal;
        this.maxVal = maxVal;
    }

    public ClickHouseBetweenParametersProvider ofBatchNum(int batchNum) {
        checkArgument(batchNum > 0, "Batch number must be positive");

        long maxElemCount = (maxVal - minVal) + 1;
        if (batchNum > maxElemCount) {
            batchNum = (int) maxElemCount;
        }
        this.batchNum = batchNum;
        this.batchSize = new Double(Math.ceil((double) maxElemCount / batchNum)).longValue();
        return this;
    }

    @Override
    public Serializable[][] getParameterValues() {
        checkState(
                batchSize > 0,
                "Batch size and batch number must be positive. Have you called `ofBatchSize` or `ofBatchNum`?");

        long maxElemCount = (maxVal - minVal) + 1;
        long bigBatchNum = maxElemCount - (batchSize - 1) * batchNum;

        Serializable[][] parameters = new Serializable[batchNum][2];
        long start = minVal;
        for (int i = 0; i < batchNum; i++) {
            long end = start + batchSize - 1 - (i >= bigBatchNum ? 1 : 0);
            parameters[i] = new Long[] {start, end};
            start = end + 1;
        }
        return parameters;
    }
}
