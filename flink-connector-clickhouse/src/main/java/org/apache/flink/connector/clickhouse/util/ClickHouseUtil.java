/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.clickhouse.util;

import org.apache.flink.connector.clickhouse.internal.schema.Expression;
import org.apache.flink.connector.clickhouse.internal.schema.FieldExpr;
import org.apache.flink.connector.clickhouse.internal.schema.FunctionExpr;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfig.PROPERTIES_PREFIX;
import static org.apache.flink.util.StringUtils.isNullOrWhitespaceOnly;

/** clickhouse util. */
public class ClickHouseUtil {

    public static final String EMPTY = "";

    private static final LocalDate DATE_PREFIX_OF_TIME = LocalDate.ofEpochDay(1);

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

    public static LocalDateTime toLocalDateTime(LocalTime localTime) {
        return localTime.atDate(DATE_PREFIX_OF_TIME);
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

        if (subExprLiteral.trim().isEmpty()) {
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

    /** TODO The timezone configured via `table.local-time-zone` should be used. */
    public static TimeZone getFlinkTimeZone() {
        return TimeZone.getDefault();
    }
}
