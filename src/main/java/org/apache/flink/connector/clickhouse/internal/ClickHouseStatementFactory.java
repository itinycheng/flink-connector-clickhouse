package org.apache.flink.connector.clickhouse.internal;

import org.apache.flink.connector.clickhouse.util.ClickHouseUtil;

import org.apache.commons.lang3.ArrayUtils;

import java.util.Arrays;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static org.apache.flink.connector.clickhouse.util.ClickHouseUtil.quoteIdentifier;

/** Create an insert/update/delete ClickHouse statement. */
public class ClickHouseStatementFactory {

    private static final String EMPTY = "";

    private ClickHouseStatementFactory() {}

    public static String getSelectStatement(
            String tableName, String databaseName, String[] selectFields) {
        String selectClause =
                Arrays.stream(selectFields)
                        .map(ClickHouseUtil::quoteIdentifier)
                        .collect(joining(", "));
        return String.join(
                EMPTY, "SELECT ", selectClause, " FROM ", fromTableClause(tableName, databaseName));
    }

    public static String getSelectWhereStatement(
            String tableName,
            String databaseName,
            String[] selectFields,
            String[] conditionFields) {
        String selectStatement = getSelectStatement(tableName, databaseName, selectFields);
        String whereClause =
                Arrays.stream(conditionFields)
                        .map(f -> format("%s = ?", quoteIdentifier(f)))
                        .collect(Collectors.joining(" AND "));
        return selectStatement + (conditionFields.length > 0 ? " WHERE " + whereClause : "");
    }

    public static String getInsertIntoStatement(String tableName, String[] fieldNames) {
        String columns =
                Arrays.stream(fieldNames)
                        .map(ClickHouseUtil::quoteIdentifier)
                        .collect(joining(", "));
        String placeholders = Arrays.stream(fieldNames).map((f) -> "?").collect(joining(", "));
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
            String tableName,
            String databaseName,
            String clusterName,
            String[] fieldNames,
            String[] keyFields,
            String[] partitionFields) {
        String setClause =
                Arrays.stream(fieldNames)
                        .filter(f -> !ArrayUtils.contains(keyFields, f))
                        .filter(f -> !ArrayUtils.contains(partitionFields, f))
                        .map((f) -> quoteIdentifier(f) + "=?")
                        .collect(joining(", "));
        String conditionClause =
                Arrays.stream(keyFields)
                        .map((f) -> quoteIdentifier(f) + "=?")
                        .collect(joining(" AND "));
        String onClusterClause = "";
        if (clusterName != null) {
            onClusterClause = " ON CLUSTER " + quoteIdentifier(clusterName);
        }

        return String.join(
                EMPTY,
                "ALTER TABLE ",
                fromTableClause(tableName, databaseName),
                onClusterClause,
                " UPDATE ",
                setClause,
                " WHERE ",
                conditionClause);
    }

    public static String getDeleteStatement(
            String tableName, String databaseName, String clusterName, String[] conditionFields) {
        String conditionClause =
                Arrays.stream(conditionFields)
                        .map((f) -> quoteIdentifier(f) + "=?")
                        .collect(joining(" AND "));
        String onClusterClause = "";
        if (clusterName != null) {
            onClusterClause = " ON CLUSTER " + quoteIdentifier(clusterName);
        }

        return String.join(
                EMPTY,
                "ALTER TABLE ",
                fromTableClause(tableName, databaseName),
                onClusterClause,
                " DELETE WHERE ",
                conditionClause);
    }

    private static String fromTableClause(String tableName, String databaseName) {
        if (databaseName == null) {
            return quoteIdentifier(tableName);
        }

        return format("%s.%s", quoteIdentifier(databaseName), quoteIdentifier(tableName));
    }
}
