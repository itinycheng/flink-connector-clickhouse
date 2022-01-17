package org.apache.flink.connector.clickhouse.split;

import static org.apache.flink.util.Preconditions.checkArgument;

/** For example, $columnName BETWEEN ? AND ? */
public class ClickHouseBatchBetweenParametersProvider extends ClickHouseBetweenParametersProvider {

    public ClickHouseBatchBetweenParametersProvider(long minVal, long maxVal) {
        super(minVal, maxVal);
    }

    @Override
    public ClickHouseBatchBetweenParametersProvider ofBatchNum(Integer batchNum) {
        checkArgument(batchNum != null && batchNum > 0, "Batch number must be positive");

        long maxElemCount = (maxVal - minVal) + 1;
        if (batchNum > maxElemCount) {
            batchNum = (int) maxElemCount;
        }
        this.batchNum = batchNum;
        return this;
    }

    @Override
    public ClickHouseBatchBetweenParametersProvider calculate() {
        this.parameterValues = divideParameterValues(batchNum);
        return this;
    }
}
