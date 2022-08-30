package com.tiny;

import org.apache.flink.connector.clickhouse.internal.partitioner.ValuePartitioner;
import org.apache.flink.connector.clickhouse.split.ClickHouseBatchBetweenParametersProvider;
import org.apache.flink.connector.clickhouse.split.ClickHouseParametersProvider;
import org.apache.flink.connector.clickhouse.split.ClickHouseShardBetweenParametersProvider;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;

import org.junit.Assert;
import org.junit.Test;
import ru.yandex.clickhouse.util.ClickHouseValueFormatter;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.TimeZone;

/** Unit test for simple App. */
public class AppTest {

    @Test
    public void timestampLtzTest() {
        Instant now = Instant.now();
        TimestampData timestampData = TimestampData.fromInstant(now);
        Timestamp timestamp = timestampData.toTimestamp();
        Instant instant = timestampData.toInstant();
        String s1 = ClickHouseValueFormatter.formatTimestamp(timestamp, TimeZone.getDefault());
        String s2 =
                ClickHouseValueFormatter.formatTimestamp(
                        Timestamp.from(instant), TimeZone.getDefault());
        System.out.println(s1);
        System.out.println(s2);
    }

    @Test
    public void timeTest() {
        LocalTime localTime = LocalTime.ofSecondOfDay(60 * 60);
        LocalDateTime localDateTime = localTime.atDate(LocalDate.ofEpochDay(1));
        Timestamp timestamp = Timestamp.valueOf(localDateTime);
        String dateTimeStr =
                ClickHouseValueFormatter.formatTimestamp(timestamp, TimeZone.getDefault());
        Assert.assertEquals("1970-01-02 01:00:00", dateTimeStr);
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
        Assert.assertEquals(shardIdValues.length, parameterValues.length);
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
        Assert.assertEquals(shardIdValues.length, parameterValues.length);
    }

    @Test
    public void partitionTest3() {
        ClickHouseParametersProvider provider =
                new ClickHouseBatchBetweenParametersProvider(-100, 100).ofBatchNum(3).calculate();
        Serializable[][] shardIdValues = provider.getShardIdValues();
        Serializable[][] parameterValues = provider.getParameterValues();
        Assert.assertNull(shardIdValues);
        Assert.assertEquals(3, parameterValues.length);
    }

    @Test
    public void valuePartitionerTest() {
        RowData.FieldGetter getter = row -> row.getDecimal(0, 20, 10);
        ValuePartitioner partitioner = new ValuePartitioner(getter);
        GenericRowData rowData = new GenericRowData(1);
        rowData.setField(0, DecimalData.fromBigDecimal(new BigDecimal("100.2313"), 20, 10));
        int select = partitioner.select(rowData, 7);
        Assert.assertEquals(2, select);
    }
}
