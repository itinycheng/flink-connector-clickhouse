package org.apache.flink.connector.clickhouse.split;

import java.io.Serializable;

/** This interface is used to compute the list of parallel query to run (i.e. splits). */
public interface ClickHouseParametersProvider {

    /** Returns the necessary parameters array to use for query in parallel a table. */
    Serializable[][] getParameterValues();
}
