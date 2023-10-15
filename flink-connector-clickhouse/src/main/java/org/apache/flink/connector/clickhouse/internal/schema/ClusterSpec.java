package org.apache.flink.connector.clickhouse.internal.schema;

import javax.annotation.Nonnull;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static java.util.stream.Collectors.toList;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Cluster. */
public class ClusterSpec implements Serializable {
    private final String name;

    private final List<ShardSpec> shards;

    private final Long weightSum;

    public ClusterSpec(@Nonnull String name, @Nonnull List<ShardSpec> shards) {
        this.name = checkNotNull(name);

        List<ShardSpec> sortedList = new ArrayList<>(shards).stream().sorted().collect(toList());
        this.shards = checkNotNull(sortedList);
        this.weightSum = sortedList.stream().mapToLong(ShardSpec::getWeight).sum();

        sortedList.forEach(
                shardSpec -> {
                    List<Long> weights =
                            sortedList.stream().map(ShardSpec::getWeight).collect(toList());
                    shardSpec.initShardRangeBound(weights);
                });
    }

    public String getName() {
        return name;
    }

    public List<ShardSpec> getShards() {
        return shards;
    }

    public Long getWeightSum() {
        return weightSum;
    }
}
