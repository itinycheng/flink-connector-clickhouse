package org.apache.flink.connector.clickhouse.internal.schema;

import javax.annotation.Nonnull;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.StringUtils.isNullOrWhitespaceOnly;

/** Column expression. */
public class FieldExpr extends Expression {

    private final String columnName;

    private FieldExpr(String columnName) {
        checkArgument(!isNullOrWhitespaceOnly(columnName), "columnName cannot be null or empty");
        this.columnName = columnName;
    }

    public static FieldExpr of(@Nonnull String columnName) {
        return new FieldExpr(columnName);
    }

    @Override
    public String explain() {
        return columnName;
    }
}
