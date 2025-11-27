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
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.io.IOException;

/** A rich sink function to write {@link RowData} records into ClickHouse. */
@Internal
public class ClickHouseRowDataSink implements Sink<RowData>, CheckpointedFunction {

    private final AbstractClickHouseOutputFormat outputFormat;

    public ClickHouseRowDataSink(@Nonnull AbstractClickHouseOutputFormat outputFormat) {
        this.outputFormat = Preconditions.checkNotNull(outputFormat);
    }

    @Override
    public SinkWriter<RowData> createWriter(WriterInitContext initContext) throws IOException {
        return new ClickHouseRowDataSinkWriter(initContext, outputFormat);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) {}

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        outputFormat.flush();
    }
}

class ClickHouseRowDataSinkWriter implements SinkWriter<RowData> {
    private final AbstractClickHouseOutputFormat outputFormat;

    public ClickHouseRowDataSinkWriter(
            WriterInitContext context, AbstractClickHouseOutputFormat outputFormat)
            throws IOException {
        this.outputFormat = outputFormat;
        this.outputFormat.open();
    }

    @Override
    public void write(RowData value, Context context) throws IOException, InterruptedException {
        outputFormat.writeRecord(value);
    }

    @Override
    public void close() throws Exception {
        outputFormat.close();
    }

    @Override
    public void flush(boolean b) throws IOException, InterruptedException {
        outputFormat.flush();
    }
}
