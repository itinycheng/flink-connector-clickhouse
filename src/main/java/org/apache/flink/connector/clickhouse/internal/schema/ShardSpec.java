package org.apache.flink.connector.clickhouse.internal.schema;

import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Shard. */
public class ShardSpec implements Comparable<ShardSpec>, Serializable {
    private final Long num;

    private final Long weight;

    private final List<ReplicaSpec> replicas;

    private Long shardLowerBound;

    private Long shardUpperBound;

    public ShardSpec(@Nonnull Long num, @Nonnull Long weight, @Nonnull List<ReplicaSpec> replicas) {
        this.num = checkNotNull(num);
        this.weight = checkNotNull(weight);
        this.replicas = checkNotNull(new ArrayList<>(replicas).stream().sorted().collect(toList()));
    }

    public String getJdbcUrls() {
        return replicas.stream()
                .map(replicaSpec -> replicaSpec.getHost() + ":" + replicaSpec.getPort())
                .collect(joining(",", "clickhouse://", ""));
    }

    public void initShardRangeBound(List<Long> weights) {
        Preconditions.checkState(
                weights.size() >= this.num,
                "Shard number must be less than or equal to shard amount.");
        shardLowerBound = weights.stream().mapToLong(value -> value).limit(this.num - 1).sum();
        shardUpperBound = weights.stream().mapToLong(value -> value).limit(this.num).sum();
    }

    @Override
    public int compareTo(ShardSpec shardSpec) {
        return (int) (this.getNum() - shardSpec.getNum());
    }

    public Long getNum() {
        return num;
    }

    public Long getWeight() {
        return weight;
    }

    public List<ReplicaSpec> getReplicas() {
        return replicas;
    }

    public Long getShardLowerBound() {
        return shardLowerBound;
    }

    public Long getShardUpperBound() {
        return shardUpperBound;
    }
}
