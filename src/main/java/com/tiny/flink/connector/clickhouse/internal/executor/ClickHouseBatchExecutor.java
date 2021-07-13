//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package com.tiny.flink.connector.clickhouse.internal.executor;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import org.apache.flink.shaded.guava18.com.google.common.util.concurrent.AbstractExecutionThreadService;

import com.tiny.flink.connector.clickhouse.internal.connection.ClickHouseConnectionProvider;
import com.tiny.flink.connector.clickhouse.internal.converter.ClickHouseRowConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHousePreparedStatement;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/** ClickHouse's batch executor. */
public class ClickHouseBatchExecutor implements ClickHouseExecutor {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseBatchExecutor.class);

    private transient ClickHousePreparedStatement stmt;

    private transient ClickHouseConnectionProvider connectionProvider;

    private transient RuntimeContext context;

    private final TypeInformation<RowData> rowDataTypeInformation;

    private final String sql;

    private final ClickHouseRowConverter converter;

    private transient List<RowData> batch;

    private final Duration flushInterval;

    private final int batchSize;

    private final int maxRetries;

    private transient TypeSerializer<RowData> typeSerializer;

    private boolean objectReuseEnabled = false;

    private transient ClickHouseBatchExecutor.ExecuteBatchService service;

    public ClickHouseBatchExecutor(
            String sql,
            ClickHouseRowConverter converter,
            Duration flushInterval,
            int batchSize,
            int maxRetries,
            TypeInformation<RowData> rowDataTypeInformation) {
        this.sql = sql;
        this.converter = converter;
        this.flushInterval = flushInterval;
        this.batchSize = batchSize;
        this.maxRetries = maxRetries;
        this.rowDataTypeInformation = rowDataTypeInformation;
    }

    @Override
    public void prepareStatement(ClickHouseConnection connection) throws SQLException {
        this.batch = new ArrayList<>();
        this.stmt = (ClickHousePreparedStatement) connection.prepareStatement(this.sql);
        if (this.service == null) {
            this.service = new ClickHouseBatchExecutor.ExecuteBatchService();
            if (!this.service.isRunning()) {
                this.service.startAsync();
            }
        }
    }

    @Override
    public void prepareStatement(ClickHouseConnectionProvider connectionProvider)
            throws SQLException {
        this.connectionProvider = connectionProvider;
        this.batch = new ArrayList<>();
        this.stmt =
                (ClickHousePreparedStatement)
                        connectionProvider.getConnection().prepareStatement(this.sql);
        if (this.service == null) {
            this.service = new ClickHouseBatchExecutor.ExecuteBatchService();
            if (!this.service.isRunning()) {
                this.service.startAsync();
            }
        }
    }

    @Override
    public void setRuntimeContext(RuntimeContext context) {
        this.context = context;
        this.typeSerializer =
                this.rowDataTypeInformation.createSerializer(context.getExecutionConfig());
        this.objectReuseEnabled = context.getExecutionConfig().isObjectReuseEnabled();
    }

    @Override
    public synchronized void addBatch(RowData record) {
        if (record.getRowKind() != RowKind.DELETE && record.getRowKind() != RowKind.UPDATE_BEFORE) {
            if (this.objectReuseEnabled) {
                this.batch.add(this.typeSerializer.copy(record));
            } else {
                this.batch.add(record);
            }
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

    private void attemptExecuteBatch() throws IOException {
        int i = 1;

        while (i <= this.maxRetries) {
            try {
                this.stmt.executeBatch();
                this.stmt.clearBatch();
                this.batch.clear();
                break;
            } catch (SQLException var5) {
                LOG.error("ClickHouse executeBatch error, retry times = {}", i, var5);
                if (i >= this.maxRetries) {
                    throw new IOException(var5);
                }

                try {
                    Thread.sleep(1000 * i);
                } catch (InterruptedException var4) {
                    Thread.currentThread().interrupt();
                    throw new IOException(
                            "unable to flush; interrupted while doing another attempt", var5);
                }

                ++i;
            }
        }
    }

    @Override
    public void closeStatement() throws SQLException {
        if (this.service != null) {
            this.service.stopAsync().awaitTerminated();
        } else {
            LOG.warn("executor closed before initialized");
        }

        if (this.stmt != null) {
            this.stmt.close();
            this.stmt = null;
        }
    }

    private class ExecuteBatchService extends AbstractExecutionThreadService {
        private ExecuteBatchService() {}

        @Override
        protected void run() throws Exception {
            while (this.isRunning()) {
                synchronized (ClickHouseBatchExecutor.this) {
                    ClickHouseBatchExecutor.this.wait(
                            ClickHouseBatchExecutor.this.flushInterval.toMillis());
                    if (!ClickHouseBatchExecutor.this.batch.isEmpty()) {
                        for (RowData r : ClickHouseBatchExecutor.this.batch) {
                            ClickHouseBatchExecutor.this.converter.toClickHouse(
                                    r, ClickHouseBatchExecutor.this.stmt);
                            ClickHouseBatchExecutor.this.stmt.addBatch();
                        }

                        this.attemptExecuteBatch();
                    }
                }
            }
        }

        private void attemptExecuteBatch() throws IOException {
            int i = 1;

            while (i <= ClickHouseBatchExecutor.this.maxRetries) {
                try {
                    ClickHouseBatchExecutor.this.stmt.executeBatch();
                    ClickHouseBatchExecutor.this.batch.clear();
                    break;
                } catch (SQLException var5) {
                    ClickHouseBatchExecutor.LOG.error(
                            "ClickHouse executeBatch error, retry times = {}", i, var5);
                    if (i >= ClickHouseBatchExecutor.this.maxRetries) {
                        throw new IOException(var5);
                    }

                    try {
                        Thread.sleep(1000 * i);
                    } catch (InterruptedException var4) {
                        Thread.currentThread().interrupt();
                        throw new IOException(
                                "unable to flush; interrupted while doing another attempt", var5);
                    }

                    ++i;
                }
            }
        }
    }
}
