//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package com.tiny.flink.connector.clickhouse.internal;

import com.tiny.flink.connector.clickhouse.internal.connection.ClickHouseConnectionProvider;
import com.tiny.flink.connector.clickhouse.internal.executor.ClickHouseExecutor;
import com.tiny.flink.connector.clickhouse.internal.options.ClickHouseOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.ClickHouseConnection;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.sql.SQLException;

/**
 * @author tiger
 */
public class ClickHouseBatchOutputFormat extends AbstractClickHouseOutputFormat {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseBatchOutputFormat.class);

    private final ClickHouseConnectionProvider connectionProvider;

    private transient ClickHouseConnection connection;

    private final ClickHouseExecutor executor;

    private final ClickHouseOptions options;

    private transient boolean closed = false;

    private transient int batchCount = 0;

    protected ClickHouseBatchOutputFormat(@Nonnull ClickHouseConnectionProvider connectionProvider,
                                          @Nonnull ClickHouseExecutor executor,
                                          @Nonnull ClickHouseOptions options) {
        this.connectionProvider = Preconditions.checkNotNull(connectionProvider);
        this.executor = Preconditions.checkNotNull(executor);
        this.options = Preconditions.checkNotNull(options);
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        try {
            this.connection = this.connectionProvider.getConnection();
            this.executor.prepareStatement(this.connectionProvider);
            this.executor.setRuntimeContext(this.getRuntimeContext());
        } catch (Exception var4) {
            throw new IOException("unable to establish connection with ClickHouse", var4);
        }
    }

    @Override
    public void writeRecord(RowData record) throws IOException {
        this.addBatch(record);
        ++this.batchCount;
        if (this.batchCount >= this.options.getBatchSize()) {
            this.flush();
        }

    }

    private void addBatch(RowData record) throws IOException {
        this.executor.addBatch(record);
    }

    @Override
    public void flush() throws IOException {
        this.executor.executeBatch();
    }

    @Override
    public void close() {
        if (!this.closed) {
            this.closed = true;

            try {
                this.flush();
            } catch (Exception var2) {
                LOG.warn("Writing records to ClickHouse failed.", var2);
            }

            this.closeConnection();
        }

    }

    private void closeConnection() {
        if (this.connection != null) {
            try {
                this.executor.closeStatement();
                this.connectionProvider.closeConnections();
            } catch (SQLException var5) {
                LOG.warn("ClickHouse connection could not be closed: {}", var5.getMessage());
            } finally {
                this.connection = null;
            }
        }

    }
}
