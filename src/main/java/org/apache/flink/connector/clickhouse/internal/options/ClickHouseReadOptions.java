package org.apache.flink.connector.clickhouse.internal.options;

import javax.annotation.Nullable;

/** ClickHouse read options. */
public class ClickHouseReadOptions extends ClickHouseConnectionOptions {

    private static final long serialVersionUID = 1L;

    private final boolean useLocal;

    private final String partitionColumn;
    private final Integer partitionNum;
    private final Long partitionLowerBound;
    private final Long partitionUpperBound;

    private ClickHouseReadOptions(
            String url,
            @Nullable String username,
            @Nullable String password,
            String databaseName,
            String tableName,
            boolean useLocal,
            String partitionColumn,
            Integer partitionNum,
            Long partitionLowerBound,
            Long partitionUpperBound) {
        super(url, username, password, databaseName, tableName);
        this.useLocal = useLocal;
        this.partitionColumn = partitionColumn;
        this.partitionNum = partitionNum;
        this.partitionLowerBound = partitionLowerBound;
        this.partitionUpperBound = partitionUpperBound;
    }

    public boolean isUseLocal() {
        return useLocal;
    }

    public String getPartitionColumn() {
        return partitionColumn;
    }

    public Integer getPartitionNum() {
        return partitionNum;
    }

    public Long getPartitionLowerBound() {
        return partitionLowerBound;
    }

    public Long getPartitionUpperBound() {
        return partitionUpperBound;
    }

    /** Builder for {@link ClickHouseReadOptions}. */
    public static class Builder {
        private String url;
        private String username;
        private String password;
        private String databaseName;
        private String tableName;
        private boolean useLocal;
        private String partitionColumn;
        private Integer partitionNum;
        private Long partitionLowerBound;
        private Long partitionUpperBound;

        public ClickHouseReadOptions.Builder withUrl(String url) {
            this.url = url;
            return this;
        }

        public ClickHouseReadOptions.Builder withUsername(String username) {
            this.username = username;
            return this;
        }

        public ClickHouseReadOptions.Builder withPassword(String password) {
            this.password = password;
            return this;
        }

        public ClickHouseReadOptions.Builder withDatabaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public ClickHouseReadOptions.Builder withTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public ClickHouseReadOptions.Builder withUseLocal(boolean useLocal) {
            this.useLocal = useLocal;
            return this;
        }

        public ClickHouseReadOptions.Builder withPartitionColumn(String partitionColumn) {
            this.partitionColumn = partitionColumn;
            return this;
        }

        public ClickHouseReadOptions.Builder withPartitionNum(Integer partitionNum) {
            this.partitionNum = partitionNum;
            return this;
        }

        public Builder withPartitionLowerBound(Long partitionLowerBound) {
            this.partitionLowerBound = partitionLowerBound;
            return this;
        }

        public Builder withPartitionUpperBound(Long partitionUpperBound) {
            this.partitionUpperBound = partitionUpperBound;
            return this;
        }

        public ClickHouseReadOptions build() {
            return new ClickHouseReadOptions(
                    url,
                    username,
                    password,
                    databaseName,
                    tableName,
                    useLocal,
                    partitionColumn,
                    partitionNum,
                    partitionLowerBound,
                    partitionUpperBound);
        }
    }
}
