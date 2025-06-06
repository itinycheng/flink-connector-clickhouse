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

package org.apache.flink.connector.clickhouse.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.io.IOException;

/** A rich sink function to write {@link RowData} records into ClickHouse. */
@Internal
public class ClickHouseRowDataSinkFunction extends RichSinkFunction<RowData>
        implements CheckpointedFunction {

    private final AbstractClickHouseOutputFormat outputFormat;

    public ClickHouseRowDataSinkFunction(@Nonnull AbstractClickHouseOutputFormat outputFormat) {
        this.outputFormat = Preconditions.checkNotNull(outputFormat);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        outputFormat.configure(parameters);
        RuntimeContext runtimeContext = getRuntimeContext();
        outputFormat.setRuntimeContext(runtimeContext);
        outputFormat.open(
                runtimeContext.getIndexOfThisSubtask(),
                runtimeContext.getNumberOfParallelSubtasks());
    }

    @Override
    public void invoke(RowData value, Context context) throws IOException {
        outputFormat.writeRecord(value);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) {}

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        outputFormat.flush();
    }

    @Override
    public void close() {
        outputFormat.close();
    }
}
