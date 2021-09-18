package org.apache.flink.connector.clickhouse.util;

import org.apache.http.client.utils.URIBuilder;

import javax.annotation.Nullable;

/** clickhouse util. */
public class ClickHouseUtil {

    public static String getJdbcUrl(String url, @Nullable String database) {
        try {
            database = database != null ? database : "";
            return "jdbc:" + (new URIBuilder(url)).setPath("/" + database).build().toString();
        } catch (Exception e) {
            throw new IllegalStateException(String.format("Cannot parse url: %s", url), e);
        }
    }
}
