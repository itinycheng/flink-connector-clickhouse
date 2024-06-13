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

package org.apache.flink.connector.clickhouse;

import org.apache.flink.connector.clickhouse.internal.partitioner.ValuePartitioner;
import org.apache.flink.connector.clickhouse.internal.schema.ClusterSpec;
import org.apache.flink.connector.clickhouse.internal.schema.ShardSpec;
import org.apache.flink.connector.clickhouse.split.ClickHouseBatchBetweenParametersProvider;
import org.apache.flink.connector.clickhouse.split.ClickHouseParametersProvider;
import org.apache.flink.connector.clickhouse.split.ClickHouseShardBetweenParametersProvider;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;

import com.clickhouse.data.value.ClickHouseDateTimeValue;
import org.junit.Ignore;
import org.junit.Test;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.TimeZone;
import java.util.regex.Matcher;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.connector.clickhouse.util.ClickHouseJdbcUtil.DISTRIBUTED_TABLE_ENGINE_PATTERN;
import static org.apache.flink.connector.clickhouse.util.ClickHouseUtil.parseShardingKey;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

/** Unit test for simple App. */
public class AppTest {

    @Ignore
    @Test
    public void timestampLtzTest() {
        Instant now = Instant.now();
        TimestampData timestampData = TimestampData.fromInstant(now);
        Instant instant = timestampData.toInstant();
        LocalDateTime localDateTime = timestampData.toLocalDateTime();
        // Error if timezone equals UTC+0.
        LocalDateTime zoneDateTime =
                instant.atZone(TimeZone.getDefault().toZoneId()).toLocalDateTime();
        assertFalse(Duration.between(localDateTime, zoneDateTime).isZero());
    }

    @Test
    public void timeTest() {
        LocalTime localTime = LocalTime.ofSecondOfDay(60 * 60);
        LocalDateTime localDateTime = localTime.atDate(LocalDate.ofEpochDay(1));
        String dateTimeStr =
                ClickHouseDateTimeValue.of(localDateTime, 0, TimeZone.getDefault()).asString();
        assertEquals("1970-01-02 01:00:00", dateTimeStr);
    }

    @Test
    public void partitionTest1() {
        ClickHouseShardBetweenParametersProvider provider =
                new ClickHouseShardBetweenParametersProvider(
                                -100, 100, new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11})
                        .ofBatchNum(23)
                        .calculate();
        Serializable[][] shardIdValues = provider.getShardIdValues();
        Serializable[][] parameterValues = provider.getParameterValues();
        assertEquals(shardIdValues.length, parameterValues.length);
    }

    @Test
    public void partitionTest2() {
        ClickHouseParametersProvider provider =
                new ClickHouseShardBetweenParametersProvider(
                                -100, -100, new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11})
                        .ofBatchNum(3)
                        .calculate();
        Serializable[][] shardIdValues = provider.getShardIdValues();
        Serializable[][] parameterValues = provider.getParameterValues();
        assertEquals(shardIdValues.length, parameterValues.length);
    }

    @Test
    public void partitionTest3() {
        ClickHouseParametersProvider provider =
                new ClickHouseBatchBetweenParametersProvider(-100, 100).ofBatchNum(3).calculate();
        Serializable[][] shardIdValues = provider.getShardIdValues();
        Serializable[][] parameterValues = provider.getParameterValues();
        assertNull(shardIdValues);
        assertEquals(3, parameterValues.length);
    }

    @Test
    public void valuePartitionerTest() {
        RowData.FieldGetter getter = row -> row.getDecimal(0, 20, 10);
        ValuePartitioner partitioner = new ValuePartitioner(Collections.singletonList(getter));
        GenericRowData rowData = new GenericRowData(1);
        rowData.setField(0, DecimalData.fromBigDecimal(new BigDecimal("100.2313"), 20, 10));

        ClusterSpec clusterSpec =
                new ClusterSpec(
                        "test",
                        Arrays.asList(
                                new ShardSpec(1, 1L, new ArrayList<>()),
                                new ShardSpec(2, 1L, new ArrayList<>())));
        int select = partitioner.select(rowData, clusterSpec);
        assertEquals(1, select);
    }

    @Test
    public void parseEngineFullTest() {
        String engineFull =
                "Distributed('cluster_name', 'database_name', 'table_name')"
                        .replaceAll("'|\\s", "");
        Matcher matcher = DISTRIBUTED_TABLE_ENGINE_PATTERN.matcher(engineFull);
        if (matcher.find()) {
            assertEquals("cluster_name", matcher.group("cluster"));
            assertEquals("database_name", matcher.group("database"));
            assertEquals("table_name", matcher.group("table"));
            assertNull(matcher.group("shardingKey"));
        }

        engineFull =
                "Distributed('cluster_name', 'database_name', 'table_name', 'id')"
                        .replaceAll("'|\\s", "");
        matcher = DISTRIBUTED_TABLE_ENGINE_PATTERN.matcher(engineFull);
        if (matcher.find()) {
            assertEquals("cluster_name", matcher.group("cluster"));
            assertEquals("database_name", matcher.group("database"));
            assertEquals("table_name", matcher.group("table"));
            assertEquals(
                    "id", requireNonNull(parseShardingKey(matcher.group("shardingKey"))).explain());
        }

        engineFull =
                "Distributed('cluster_name', 'database_name', 'table_name', rand())"
                        .replaceAll("'|\\s", "");
        matcher = DISTRIBUTED_TABLE_ENGINE_PATTERN.matcher(engineFull);
        if (matcher.find()) {
            assertEquals("cluster_name", matcher.group("cluster"));
            assertEquals("database_name", matcher.group("database"));
            assertEquals("table_name", matcher.group("table"));
            assertEquals(
                    "rand()",
                    requireNonNull(parseShardingKey(matcher.group("shardingKey"))).explain());
        }

        engineFull =
                "Distributed('cluster_name', 'database_name', 'table_name', javaHash(id))"
                        .replaceAll("'|\\s", "");
        matcher = DISTRIBUTED_TABLE_ENGINE_PATTERN.matcher(engineFull);
        if (matcher.find()) {
            assertEquals("cluster_name", matcher.group("cluster"));
            assertEquals("database_name", matcher.group("database"));
            assertEquals("table_name", matcher.group("table"));
            assertEquals(
                    "javaHash(id)",
                    requireNonNull(parseShardingKey(matcher.group("shardingKey"))).explain());
        }

        engineFull =
                "Distributed('cluster_name', 'database_name', 'table_name', plus(id1, id2))"
                        .replaceAll("'|\\s", "");
        matcher = DISTRIBUTED_TABLE_ENGINE_PATTERN.matcher(engineFull);
        if (matcher.find()) {
            assertEquals("cluster_name", matcher.group("cluster"));
            assertEquals("database_name", matcher.group("database"));
            assertEquals("table_name", matcher.group("table"));
            assertEquals(
                    "plus(id1,id2)",
                    requireNonNull(parseShardingKey(matcher.group("shardingKey"))).explain());
        }

        engineFull =
                "Distributed('cluster_name', 'database_name', 'table_name', javaHash(toString(date, age)))"
                        .replaceAll("'|\\s", "");
        matcher = DISTRIBUTED_TABLE_ENGINE_PATTERN.matcher(engineFull);
        if (matcher.find()) {
            assertEquals("cluster_name", matcher.group("cluster"));
            assertEquals("database_name", matcher.group("database"));
            assertEquals("table_name", matcher.group("table"));
            assertEquals(
                    "javaHash(toString(date,age))",
                    requireNonNull(parseShardingKey(matcher.group("shardingKey"))).explain());
        }

        engineFull =
                "Distributed('cluster_name', 'database_name', 'table_name', javaHash(toString(date, age)), policy_name)"
                        .replaceAll("'|\\s", "");
        matcher = DISTRIBUTED_TABLE_ENGINE_PATTERN.matcher(engineFull);
        if (matcher.find()) {
            assertEquals("cluster_name", matcher.group("cluster"));
            assertEquals("database_name", matcher.group("database"));
            assertEquals("table_name", matcher.group("table"));
            assertEquals(
                    "javaHash(toString(date,age))",
                    requireNonNull(parseShardingKey(matcher.group("shardingKey"))).explain());
        }
    }
}
