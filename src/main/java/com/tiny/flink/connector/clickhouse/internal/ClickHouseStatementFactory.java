//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package com.tiny.flink.connector.clickhouse.internal;

import java.util.Arrays;
import java.util.stream.Collectors;

/** Create an insert/update/delete ClickHouse statement. */
public class ClickHouseStatementFactory {

    private static final String EMPTY = "";

    private ClickHouseStatementFactory() {}

    public static String getInsertIntoStatement(String tableName, String[] fieldNames) {
        String columns =
                Arrays.stream(fieldNames)
                        .map(ClickHouseStatementFactory::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        String placeholders =
                Arrays.stream(fieldNames).map((f) -> "?").collect(Collectors.joining(", "));
        return String.join(
                EMPTY,
                "INSERT INTO ",
                quoteIdentifier(tableName),
                "(",
                columns,
                ") VALUES (",
                placeholders,
                ")");
    }

    public static String getUpdateStatement(
            String tableName, String[] fieldNames, String[] conditionFields, String clusterName) {
        String setClause =
                Arrays.stream(fieldNames)
                        .map((f) -> quoteIdentifier(f) + "=?")
                        .collect(Collectors.joining(", "));
        String conditionClause =
                Arrays.stream(conditionFields)
                        .map((f) -> quoteIdentifier(f) + "=?")
                        .collect(Collectors.joining(" AND "));
        String onClusterClause = "";
        if (clusterName != null) {
            onClusterClause = " ON CLUSTER " + quoteIdentifier(clusterName);
        }

        return String.join(
                EMPTY,
                "ALTER TABLE ",
                quoteIdentifier(tableName),
                onClusterClause,
                " UPDATE ",
                setClause,
                " WHERE ",
                conditionClause);
    }

    public static String getDeleteStatement(
            String tableName, String[] conditionFields, String clusterName) {
        String conditionClause =
                Arrays.stream(conditionFields)
                        .map((f) -> quoteIdentifier(f) + "=?")
                        .collect(Collectors.joining(" AND "));
        String onClusterClause = "";
        if (clusterName != null) {
            onClusterClause = " ON CLUSTER " + quoteIdentifier(clusterName);
        }

        return String.join(
                EMPTY,
                "ALTER TABLE ",
                quoteIdentifier(tableName),
                onClusterClause,
                " DELETE WHERE ",
                conditionClause);
    }

    public static String getRowExistsStatement(String tableName, String[] conditionFields) {
        String fieldExpressions =
                Arrays.stream(conditionFields)
                        .map((f) -> quoteIdentifier(f) + "=?")
                        .collect(Collectors.joining(" AND "));
        return String.join(
                EMPTY, "SELECT 1 FROM ", quoteIdentifier(tableName), " WHERE ", fieldExpressions);
    }

    public static String quoteIdentifier(String identifier) {
        return String.join(EMPTY, "`", identifier, "`");
    }
}
