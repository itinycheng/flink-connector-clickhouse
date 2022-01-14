//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.flink.connector.clickhouse.internal;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.table.data.RowData;

import java.io.IOException;

/** Output data to ClickHouse local table. */
public class ClickHouseShardInputFormat extends AbstractClickHouseInputFormat {

    public ClickHouseShardInputFormat() {
        super(null);
    }

    @Override
    public InputSplit[] createInputSplits(int minNumSplits) throws IOException {
        return new InputSplit[0];
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
        return null;
    }

    @Override
    public void open(InputSplit split) throws IOException {}

    @Override
    public boolean reachedEnd() throws IOException {
        return false;
    }

    @Override
    public RowData nextRecord(RowData reuse) throws IOException {
        return null;
    }

    @Override
    public void close() throws IOException {}
}
