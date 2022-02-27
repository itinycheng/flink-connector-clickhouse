package org.apache.flink.connector.clickhouse.config;

/** clickhouse config properties. */
public class ClickHouseConfig {

    public static final String IDENTIFIER = "clickhouse";

    public static final String PROPERTIES_PREFIX = "properties.";

    public static final String URL = "url";

    public static final String USERNAME = "username";

    public static final String PASSWORD = "password";

    public static final String DATABASE_NAME = "database-name";

    public static final String TABLE_NAME = "table-name";

    public static final String USE_LOCAL = "use-local";

    public static final String SINK_BATCH_SIZE = "sink.batch-size";

    public static final String SINK_FLUSH_INTERVAL = "sink.flush-interval";

    public static final String SINK_MAX_RETRIES = "sink.max-retries";

    @Deprecated public static final String SINK_WRITE_LOCAL = "sink.write-local";

    public static final String SINK_PARTITION_STRATEGY = "sink.partition-strategy";

    public static final String SINK_PARTITION_KEY = "sink.partition-key";

    public static final String SINK_IGNORE_DELETE = "sink.ignore-delete";

    public static final String CATALOG_IGNORE_PRIMARY_KEY = "catalog.ignore-primary-key";

    public static final String SCAN_PARTITION_COLUMN = "scan.partition.column";

    public static final String SCAN_PARTITION_NUM = "scan.partition.num";

    public static final String SCAN_PARTITION_LOWER_BOUND = "scan.partition.lower-bound";

    public static final String SCAN_PARTITION_UPPER_BOUND = "scan.partition.upper-bound";
}
