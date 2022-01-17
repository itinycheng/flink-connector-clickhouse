package org.apache.flink.connector.clickhouse.internal.options;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Optional;

/** ClickHouse connection options. */
public class ClickHouseConnectionOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String url;

    private final String username;

    private final String password;

    private final String databaseName;

    private final String tableName;

    protected ClickHouseConnectionOptions(
            String url,
            @Nullable String username,
            @Nullable String password,
            String databaseName,
            String tableName) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.databaseName = databaseName;
        this.tableName = tableName;
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
}
