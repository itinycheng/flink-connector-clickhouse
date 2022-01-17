package org.apache.flink.connector.clickhouse.split;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/** This class is used to compute the list of parallel query to run (i.e. splits). */
public abstract class ClickHouseBetweenParametersProvider extends ClickHouseParametersProvider {

    private static final String BETWEEN_CLAUSE = "`%s` BETWEEN ? AND ?";

    protected final long minVal;

    protected final long maxVal;

    public ClickHouseBetweenParametersProvider(long minVal, long maxVal) {
        checkArgument(maxVal >= minVal, "maxVal must be larger than minVal");
        this.minVal = minVal;
        this.maxVal = maxVal;
    }

    @Override
    public String getParameterClause() {
        return BETWEEN_CLAUSE;
    }

    protected Serializable[][] divideParameterValues(int batchNum) {
        long maxElemCount = (maxVal - minVal) + 1;
        long batchSize = new Double(Math.ceil((double) maxElemCount / batchNum)).longValue();
        long bigBatchNum = maxElemCount - (batchSize - 1) * batchNum;

        checkState(batchSize > 0, "Batch size and batch number must be positive.");

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
