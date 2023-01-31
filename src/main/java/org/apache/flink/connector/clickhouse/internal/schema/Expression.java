package org.apache.flink.connector.clickhouse.internal.schema;

/** Expression. */
public abstract class Expression {
    public abstract String explain();
}
