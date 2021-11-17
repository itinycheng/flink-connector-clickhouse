package com.tiny;

import org.apache.flink.table.data.TimestampData;

import org.junit.Test;
import ru.yandex.clickhouse.util.ClickHouseValueFormatter;

import java.sql.Timestamp;
import java.time.Instant;
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
}
