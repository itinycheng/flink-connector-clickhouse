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

package org.apache.flink.connector.clickhouse.catalog;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.connector.source.lookup.LookupOptions;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.connector.clickhouse.config.ClickHouseConfig.IDENTIFIER;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfig.PROPERTIES_PREFIX;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.CATALOG_IGNORE_PRIMARY_KEY;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.DATABASE_NAME;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.PASSWORD;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SCAN_PARTITION_COLUMN;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SCAN_PARTITION_LOWER_BOUND;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SCAN_PARTITION_NUM;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SCAN_PARTITION_UPPER_BOUND;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SINK_BATCH_SIZE;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SINK_FLUSH_INTERVAL;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SINK_IGNORE_DELETE;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SINK_MAX_RETRIES;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SINK_PARALLELISM;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SINK_PARTITION_KEY;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SINK_PARTITION_STRATEGY;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SINK_SHARDING_USE_TABLE_DEF;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SINK_UPDATE_STRATEGY;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.URL;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.USERNAME;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.USE_LOCAL;
import static org.apache.flink.table.factories.FactoryUtil.PROPERTY_VERSION;

/** Factory for {@link ClickHouseCatalog}. */
public class ClickHouseCatalogFactory implements CatalogFactory {

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(URL);
        options.add(USERNAME);
        options.add(PASSWORD);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PROPERTY_VERSION);
        options.add(DATABASE_NAME);
        options.add(USE_LOCAL);
        options.add(CATALOG_IGNORE_PRIMARY_KEY);

        options.add(SINK_BATCH_SIZE);
        options.add(SINK_FLUSH_INTERVAL);
        options.add(SINK_MAX_RETRIES);
        options.add(SINK_UPDATE_STRATEGY);
        options.add(SINK_PARTITION_STRATEGY);
        options.add(SINK_PARTITION_KEY);
        options.add(SINK_SHARDING_USE_TABLE_DEF);
        options.add(SINK_IGNORE_DELETE);
        options.add(SINK_PARALLELISM);

        options.add(SCAN_PARTITION_COLUMN);
        options.add(SCAN_PARTITION_NUM);
        options.add(SCAN_PARTITION_LOWER_BOUND);
        options.add(SCAN_PARTITION_UPPER_BOUND);

        options.add(LookupOptions.CACHE_TYPE);
        options.add(LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_ACCESS);
        options.add(LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_WRITE);
        options.add(LookupOptions.PARTIAL_CACHE_MAX_ROWS);
        options.add(LookupOptions.PARTIAL_CACHE_CACHE_MISSING_KEY);
        options.add(LookupOptions.MAX_RETRIES);
        return options;
    }

    @Override
    public Catalog createCatalog(Context context) {
        final FactoryUtil.CatalogFactoryHelper helper =
                FactoryUtil.createCatalogFactoryHelper(this, context);
        helper.validateExcept(PROPERTIES_PREFIX);

        return new ClickHouseCatalog(
                context.getName(),
                helper.getOptions().get(DATABASE_NAME),
                helper.getOptions().get(URL),
                helper.getOptions().get(USERNAME),
                helper.getOptions().get(PASSWORD),
                helper.getOptions().toMap());
    }
}
