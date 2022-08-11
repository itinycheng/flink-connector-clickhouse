package org.apache.flink.connector.clickhouse.split;

/** Non parameters provided. */
public class ClickHouseNonParametersProvider extends ClickHouseParametersProvider {

    @Override
    public String getParameterClause() {
        return null;
    }

    @Override
    public ClickHouseParametersProvider ofBatchNum(Integer batchNum) {
        return this;
    }

    @Override
    public ClickHouseParametersProvider calculate() {
        return this;
    }
}
