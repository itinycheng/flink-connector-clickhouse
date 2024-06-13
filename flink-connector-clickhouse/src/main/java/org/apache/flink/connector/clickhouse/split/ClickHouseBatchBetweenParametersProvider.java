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

import static org.apache.flink.util.Preconditions.checkArgument;

/** For example, $columnName BETWEEN ? AND ? */
public class ClickHouseBatchBetweenParametersProvider extends ClickHouseBetweenParametersProvider {

    public ClickHouseBatchBetweenParametersProvider(long minVal, long maxVal) {
        super(minVal, maxVal);
    }

    @Override
    public ClickHouseBatchBetweenParametersProvider ofBatchNum(Integer batchNum) {
        checkArgument(batchNum != null && batchNum > 0, "Batch number must be positive");

        long maxElemCount = defaultMaxIfLTZero((maxVal - minVal) + 1);
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
