package org.apache.flink.connector.clickhouse.internal.schema;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.StringUtils.isNullOrWhitespaceOnly;

/** Distributed table engine full schema. */
public class DistributedEngineFull implements Serializable {

    private final String cluster;

    private final String database;

    private final String table;

    private final Expression shardingKey;

    private final String policyName;

    public static DistributedEngineFull of(
            String cluster,
            String database,
            String table,
            Expression shardingKey,
            String policyName) {
        return new DistributedEngineFull(cluster, database, table, shardingKey, policyName);
    }

    private DistributedEngineFull(
            String cluster,
            String database,
            String table,
            Expression shardingKey,
            String policyName) {
        checkArgument(!isNullOrWhitespaceOnly(cluster), "cluster cannot be null or empty");
        checkArgument(!isNullOrWhitespaceOnly(database), "database cannot be null or empty");
        checkArgument(!isNullOrWhitespaceOnly(table), "table cannot be null or empty");

        this.cluster = cluster;
        this.database = database;
        this.table = table;
        this.shardingKey = shardingKey;
        this.policyName = policyName;
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

    public Expression getShardingKey() {
        return shardingKey;
    }

    public String getPolicyName() {
        return policyName;
    }
}
