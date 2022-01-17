package org.apache.flink.connector.clickhouse.internal.options;

import javax.annotation.Nullable;

import java.time.Duration;

/** ClickHouse properties. */
public class ClickHouseOptions extends ClickHouseConnectionOptions {

    private static final long serialVersionUID = 1L;

    private final int batchSize;

    private final Duration flushInterval;

    private final int maxRetries;

    private final boolean useLocal;

    private final String partitionStrategy;

    private final String partitionKey;

    private final boolean ignoreDelete;

    private ClickHouseOptions(
            String url,
            @Nullable String username,
            @Nullable String password,
            String databaseName,
            String tableName,
            int batchSize,
            Duration flushInterval,
            int maxRetires,
            boolean useLocal,
            String partitionStrategy,
            String partitionKey,
            boolean ignoreDelete) {
        super(url, username, password, databaseName, tableName);
        this.batchSize = batchSize;
        this.flushInterval = flushInterval;
        this.maxRetries = maxRetires;
        this.useLocal = useLocal;
        this.partitionStrategy = partitionStrategy;
        this.partitionKey = partitionKey;
        this.ignoreDelete = ignoreDelete;
    }

    public int getBatchSize() {
        return this.batchSize;
    }

    public Duration getFlushInterval() {
        return this.flushInterval;
    }

    public int getMaxRetries() {
        return this.maxRetries;
    }

    public boolean isUseLocal() {
        return this.useLocal;
    }

    public String getPartitionStrategy() {
        return this.partitionStrategy;
    }

    public String getPartitionKey() {
        return this.partitionKey;
    }

    public boolean getIgnoreDelete() {
        return this.ignoreDelete;
    }

    /** Builder for {@link ClickHouseOptions}. */
    public static class Builder {
        private String url;
        private String username;
        private String password;
        private String databaseName;
        private String tableName;
        private int batchSize;
        private Duration flushInterval;
        private int maxRetries;
        private boolean writeLocal;
        private boolean useLocal;
        private String partitionStrategy;
        private String partitionKey;
        private boolean ignoreDelete;

        public Builder() {}

        public ClickHouseOptions.Builder withUrl(String url) {
            this.url = url;
            return this;
        }

        public ClickHouseOptions.Builder withUsername(String username) {
            this.username = username;
            return this;
        }

        public ClickHouseOptions.Builder withPassword(String password) {
            this.password = password;
            return this;
        }

        public ClickHouseOptions.Builder withDatabaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public ClickHouseOptions.Builder withTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public ClickHouseOptions.Builder withBatchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public ClickHouseOptions.Builder withFlushInterval(Duration flushInterval) {
            this.flushInterval = flushInterval;
            return this;
        }

        public ClickHouseOptions.Builder withMaxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public ClickHouseOptions.Builder withWriteLocal(Boolean writeLocal) {
            this.writeLocal = writeLocal;
            return this;
        }

        public ClickHouseOptions.Builder withUseLocal(Boolean useLocal) {
            this.useLocal = useLocal;
            return this;
        }

        public ClickHouseOptions.Builder withPartitionStrategy(String partitionStrategy) {
            this.partitionStrategy = partitionStrategy;
            return this;
        }

        public ClickHouseOptions.Builder withPartitionKey(String partitionKey) {
            this.partitionKey = partitionKey;
            return this;
        }

        public ClickHouseOptions.Builder withIgnoreDelete(boolean ignoreDelete) {
            this.ignoreDelete = ignoreDelete;
            return this;
        }

        public ClickHouseOptions build() {
            return new ClickHouseOptions(
                    url,
                    username,
                    password,
                    databaseName,
                    tableName,
                    batchSize,
                    flushInterval,
                    maxRetries,
                    writeLocal || useLocal,
                    partitionStrategy,
                    partitionKey,
                    ignoreDelete);
        }
    }
}
