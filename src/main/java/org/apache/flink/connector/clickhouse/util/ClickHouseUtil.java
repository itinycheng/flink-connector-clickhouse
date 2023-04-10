package org.apache.flink.connector.clickhouse.util;

import org.apache.flink.connector.clickhouse.internal.schema.Expression;
import org.apache.flink.connector.clickhouse.internal.schema.FieldExpr;
import org.apache.flink.connector.clickhouse.internal.schema.FunctionExpr;

import org.apache.http.client.utils.URIBuilder;

import javax.annotation.Nullable;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfig.PROPERTIES_PREFIX;
import static org.apache.flink.util.StringUtils.isNullOrWhitespaceOnly;

/** clickhouse util. */
public class ClickHouseUtil {

    public static final String EMPTY = "";

    private static final LocalDate DATE_PREFIX_OF_TIME = LocalDate.ofEpochDay(1);

    public static String getJdbcUrl(String url, @Nullable String database) {
        try {
            database = database != null ? database : "";
            return "jdbc:" + (new URIBuilder(url)).setPath("/" + database).build().toString();
        } catch (Exception e) {
            throw new IllegalStateException(String.format("Cannot parse url: %s", url), e);
        }
    }

    public static Properties getClickHouseProperties(Map<String, String> tableOptions) {
        final Properties properties = new Properties();

        tableOptions.keySet().stream()
                .filter(key -> key.startsWith(PROPERTIES_PREFIX))
                .forEach(
                        key -> {
                            final String value = tableOptions.get(key);
                            final String subKey = key.substring((PROPERTIES_PREFIX).length());
                            properties.setProperty(subKey, value);
                        });
        return properties;
    }

    public static Timestamp toEpochDayOneTimestamp(LocalTime localTime) {
        LocalDateTime localDateTime = localTime.atDate(DATE_PREFIX_OF_TIME);
        return Timestamp.valueOf(localDateTime);
    }

    public static String quoteIdentifier(String identifier) {
        return String.join(EMPTY, "`", identifier, "`");
    }

    public static Expression parseShardingKey(String shardingKey) {
        if (isNullOrWhitespaceOnly(shardingKey)) {
            return null;
        }

        if (!shardingKey.contains("(")) {
            return FieldExpr.of(shardingKey);
        }

        return parseFunctionExpr(shardingKey);
    }

    private static Expression parseFunctionExpr(String shardingExpr) {
        int bracketStartIndex = shardingExpr.indexOf("(");
        String functionName = shardingExpr.substring(0, bracketStartIndex);
        String subExprLiteral =
                shardingExpr.substring(bracketStartIndex + 1, shardingExpr.lastIndexOf(")"));

        if (subExprLiteral.trim().length() == 0) {
            return FunctionExpr.of(functionName, emptyList());
        }

        if (!subExprLiteral.contains("(")) {
            String[] subExprLiteralList = subExprLiteral.split(",");
            List<Expression> exprList = new ArrayList<>(subExprLiteralList.length);
            for (String exprLiteral : subExprLiteralList) {
                exprList.add(FieldExpr.of(exprLiteral));
            }
            return FunctionExpr.of(functionName, exprList);
        }

        Expression expression = parseFunctionExpr(subExprLiteral);
        return FunctionExpr.of(functionName, singletonList(expression));
    }

    public static String getSelectFromStatement(
            String tableName, String[] selectFields, String[] conditionFields) {
        String selectExpressions =
                Arrays.stream(selectFields)
                        .map(ClickHouseUtil::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        String fieldExpressions =
                Arrays.stream(conditionFields)
                        .map(f -> format("%s = ?", quoteIdentifier(f)))
                        .collect(Collectors.joining(" AND "));
        return "SELECT "
                + selectExpressions
                + " FROM "
                + quoteIdentifier(tableName)
                + (conditionFields.length > 0 ? " WHERE " + fieldExpressions : "");
    }
}
