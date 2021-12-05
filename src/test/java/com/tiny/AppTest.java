package com.tiny;

import org.apache.flink.table.data.TimestampData;

import org.junit.Assert;
import org.junit.Test;
import ru.yandex.clickhouse.util.ClickHouseValueFormatter;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.TimeZone;

/** Unit test for simple App. */
public class AppTest {

    /** Rigorous Test :-). */
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
}
