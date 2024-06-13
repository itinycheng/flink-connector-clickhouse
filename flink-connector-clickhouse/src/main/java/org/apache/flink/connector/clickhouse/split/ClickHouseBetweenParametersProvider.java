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

    protected long defaultMaxIfLTZero(long value) {
        return value < 0 ? Long.MAX_VALUE : value;
    }

    protected Serializable[][] divideParameterValues(int batchNum) {
        long maxElemCount = defaultMaxIfLTZero((maxVal - minVal) + 1);
        long batchSize = new Double(Math.ceil((double) maxElemCount / batchNum)).longValue();
        long bigBatchNum = maxElemCount - (batchSize - 1) * batchNum;

        checkState(batchSize > 0, "Batch size and batch number must be positive.");

        Serializable[][] parameters = new Serializable[batchNum][2];
        long start = minVal;
        for (int i = 0; i < batchNum; i++) {
            long end = start + batchSize - 1 - (i >= bigBatchNum ? 1 : 0);
            end = defaultMaxIfLTZero(end);
            parameters[i] = new Long[] {start, end};
            start = end + 1;
        }
        return parameters;
    }
}
