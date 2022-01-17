package org.apache.flink.connector.clickhouse.internal.common;

import org.apache.flink.util.StringUtils;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkArgument;

/** Distributed table engine full schema. */
public class DistributedEngineFullSchema implements Serializable {

    private final String cluster;

    private final String database;

    private final String table;

    private final String shardingKey;

    private final String policyName;

    public static DistributedEngineFullSchema of(String cluster, String database, String table) {
        return new DistributedEngineFullSchema(cluster, database, table);
    }

    public static DistributedEngineFullSchema of(
            String cluster, String database, String table, String shardingKey, String policyName) {
        return new DistributedEngineFullSchema(cluster, database, table, shardingKey, policyName);
    }

    private DistributedEngineFullSchema(
            String cluster, String database, String table, String shardingKey, String policyName) {
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(cluster), "cluster cannot be null or empty");
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(database), "database cannot be null or empty");
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(table), "table cannot be null or empty");

        this.cluster = cluster;
        this.database = database;
        this.table = table;
        this.shardingKey = shardingKey;
        this.policyName = policyName;
    }

    private DistributedEngineFullSchema(String cluster, String database, String table) {
        this(cluster, database, table, null, null);
    }

    public String getCluster() {
        return cluster;
    }

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    public String getShardingKey() {
        return shardingKey;
    }

    public String getPolicyName() {
        return policyName;
    }
}
