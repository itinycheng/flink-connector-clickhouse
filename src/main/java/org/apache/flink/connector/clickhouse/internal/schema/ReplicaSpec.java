package org.apache.flink.connector.clickhouse.internal.schema;

import javax.annotation.Nonnull;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Replica. */
public class ReplicaSpec implements Comparable<ReplicaSpec>, Serializable {

    private final Long num;

    private final String host;

    private final Integer port;

    public ReplicaSpec(@Nonnull Long num, @Nonnull String host, @Nonnull Integer port) {
        this.num = checkNotNull(num);
        this.host = checkNotNull(host);
        this.port = checkNotNull(port);
    }

    public Long getNum() {
        return num;
    }

    public String getHost() {
        return host;
    }

    public Integer getPort() {
        return port;
    }

    @Override
    public int compareTo(ReplicaSpec replicaSpec) {
        return (int) (this.getNum() - replicaSpec.getNum());
    }
}
