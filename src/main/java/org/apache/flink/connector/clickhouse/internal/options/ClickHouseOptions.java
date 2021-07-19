//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.flink.connector.clickhouse.internal.options;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Duration;
import java.util.Optional;

/** ClickHouse properties. */
public class ClickHouseOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String url;

    private final String username;

    private final String password;

    private final String databaseName;

    private final String tableName;

    private final int batchSize;

    private final Duration flushInterval;

    private final int maxRetries;

    private final boolean writeLocal;

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
            boolean writeLocal,
            String partitionStrategy,
            String partitionKey,
            boolean ignoreDelete) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.batchSize = batchSize;
        this.flushInterval = flushInterval;
        this.maxRetries = maxRetires;
        this.writeLocal = writeLocal;
        this.partitionStrategy = partitionStrategy;
        this.partitionKey = partitionKey;
        this.ignoreDelete = ignoreDelete;
    }

    public String getUrl() {
        return this.url;
    }

    public Optional<String> getUsername() {
        return Optional.ofNullable(this.username);
    }

    public Optional<String> getPassword() {
        return Optional.ofNullable(this.password);
    }

    public String getDatabaseName() {
        return this.databaseName;
    }

    public String getTableName() {
        return this.tableName;
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

    public boolean getWriteLocal() {
        return this.writeLocal;
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
                    this.url,
                    this.username,
                    this.password,
                    this.databaseName,
                    this.tableName,
                    this.batchSize,
                    this.flushInterval,
                    this.maxRetries,
                    this.writeLocal,
                    this.partitionStrategy,
                    this.partitionKey,
                    this.ignoreDelete);
        }
    }
}
