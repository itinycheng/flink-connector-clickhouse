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

    public static final String SINK_UPDATE_STRATEGY = "sink.update-strategy";

    public static final String SINK_PARTITION_STRATEGY = "sink.partition-strategy";

    public static final String SINK_PARTITION_KEY = "sink.partition-key";

    public static final String SINK_SHARDING_USE_TABLE_DEF = "sink.sharding.use-table-definition";

    public static final String SINK_IGNORE_DELETE = "sink.ignore-delete";

    public static final String CATALOG_IGNORE_PRIMARY_KEY = "catalog.ignore-primary-key";

    public static final String SCAN_PARTITION_COLUMN = "scan.partition.column";

    public static final String SCAN_PARTITION_NUM = "scan.partition.num";

    public static final String SCAN_PARTITION_LOWER_BOUND = "scan.partition.lower-bound";

    public static final String SCAN_PARTITION_UPPER_BOUND = "scan.partition.upper-bound";
}
