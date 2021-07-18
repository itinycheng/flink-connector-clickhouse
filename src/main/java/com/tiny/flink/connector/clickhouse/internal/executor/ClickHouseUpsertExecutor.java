//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package com.tiny.flink.connector.clickhouse.internal.executor;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.table.data.RowData;

import org.apache.flink.shaded.guava18.com.google.common.util.concurrent.AbstractExecutionThreadService;

import com.tiny.flink.connector.clickhouse.internal.connection.ClickHouseConnectionProvider;
import com.tiny.flink.connector.clickhouse.internal.converter.ClickHouseRowConverter;
import com.tiny.flink.connector.clickhouse.internal.options.ClickHouseOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHousePreparedStatement;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** ClickHouse's upsert executor. */
public class ClickHouseUpsertExecutor implements ClickHouseExecutor {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseUpsertExecutor.class);

    private transient ClickHousePreparedStatement insertStmt;

    private transient ClickHousePreparedStatement updateStmt;

    private transient ClickHousePreparedStatement deleteStmt;

    private final String insertSql;

    private final String updateSql;

    private final String deleteSql;

    private final ClickHouseRowConverter converter;

    private final transient List<RowData> insertBatch;

    private final transient List<RowData> updateBatch;

    private final transient List<RowData> deleteBatch;

    private transient ClickHouseUpsertExecutor.ExecuteBatchService service;

    private final Duration flushInterval;

    private final int maxRetries;

    public ClickHouseUpsertExecutor(
            String insertSql,
            String updateSql,
            String deleteSql,
            ClickHouseRowConverter converter,
            ClickHouseOptions options) {
        this.insertSql = insertSql;
        this.updateSql = updateSql;
        this.deleteSql = deleteSql;
        this.converter = converter;
        this.flushInterval = options.getFlushInterval();
        this.maxRetries = options.getMaxRetries();
        this.insertBatch = new ArrayList<>();
        this.updateBatch = new ArrayList<>();
        this.deleteBatch = new ArrayList<>();
    }

    @Override
    public void prepareStatement(ClickHouseConnection connection) throws SQLException {
        this.insertStmt = (ClickHousePreparedStatement) connection.prepareStatement(this.insertSql);
        this.updateStmt = (ClickHousePreparedStatement) connection.prepareStatement(this.updateSql);
        this.deleteStmt = (ClickHousePreparedStatement) connection.prepareStatement(this.deleteSql);
        this.service = new ClickHouseUpsertExecutor.ExecuteBatchService();
        this.service.startAsync();
    }

    @Override
    public void prepareStatement(ClickHouseConnectionProvider connectionProvider) {}

    @Override
    public void setRuntimeContext(RuntimeContext context) {}

    @Override
    public synchronized void addBatch(RowData record) {
        switch (record.getRowKind()) {
            case INSERT:
                this.insertBatch.add(record);
                break;
            case UPDATE_AFTER:
                this.updateBatch.add(record);
                break;
            case DELETE:
                this.deleteBatch.add(record);
                break;
            case UPDATE_BEFORE:
                break;
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Unknown row kind, the supported row kinds is: INSERT, UPDATE_BEFORE, UPDATE_AFTER, DELETE, but get: %s.",
                                record.getRowKind()));
        }
    }

    @Override
    public synchronized void executeBatch() throws IOException {
        if (this.service.isRunning()) {
            this.notifyAll();
        } else {
            throw new IOException("executor unexpectedly terminated", this.service.failureCause());
        }
    }

    @Override
    public void closeStatement() throws SQLException {
        if (this.service != null) {
            this.service.stopAsync().awaitTerminated();
        } else {
            LOG.warn("executor closed before initialized");
        }

        for (ClickHousePreparedStatement clickHousePreparedStatement :
                Arrays.asList(this.insertStmt, this.updateStmt, this.deleteStmt)) {
            if (clickHousePreparedStatement != null) {
                clickHousePreparedStatement.close();
            }
        }
    }

    private void attemptExecuteBatch(ClickHousePreparedStatement stmt, List<RowData> batch)
            throws IOException {
        int i = 1;

        while (i <= this.maxRetries) {
            try {
                stmt.executeBatch();
                batch.clear();
                break;
            } catch (SQLException var7) {
                LOG.error("ClickHouse executeBatch error, retry times = {}", i, var7);
                if (i >= this.maxRetries) {
                    throw new IOException(var7);
                }

                try {
                    Thread.sleep(1000 * i);
                } catch (InterruptedException var6) {
                    Thread.currentThread().interrupt();
                    throw new IOException(
                            "unable to flush; interrupted while doing another attempt", var7);
                }

                ++i;
            }
        }
    }

    private class ExecuteBatchService extends AbstractExecutionThreadService {

        private ExecuteBatchService() {}

        @Override
        protected void run() throws Exception {
            while (this.isRunning()) {
                synchronized (ClickHouseUpsertExecutor.this) {
                    ClickHouseUpsertExecutor.this.wait(
                            ClickHouseUpsertExecutor.this.flushInterval.toMillis());
                    this.processBatch(
                            ClickHouseUpsertExecutor.this.insertStmt,
                            ClickHouseUpsertExecutor.this.insertBatch);
                    this.processBatch(
                            ClickHouseUpsertExecutor.this.updateStmt,
                            ClickHouseUpsertExecutor.this.updateBatch);
                    this.processBatch(
                            ClickHouseUpsertExecutor.this.deleteStmt,
                            ClickHouseUpsertExecutor.this.deleteBatch);
                }
            }
        }

        private void processBatch(ClickHousePreparedStatement stmt, List<RowData> batch)
                throws SQLException, IOException {
            if (!batch.isEmpty()) {

                for (RowData r : ClickHouseUpsertExecutor.this.insertBatch) {
                    ClickHouseUpsertExecutor.this.converter.toExternal(r, stmt);
                    stmt.addBatch();
                }

                ClickHouseUpsertExecutor.this.attemptExecuteBatch(stmt, batch);
            }
        }
    }
}
