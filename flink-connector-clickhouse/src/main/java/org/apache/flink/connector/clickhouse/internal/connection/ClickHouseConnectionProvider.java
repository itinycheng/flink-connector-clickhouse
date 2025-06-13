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

package org.apache.flink.connector.clickhouse.internal.connection;

import org.apache.flink.connector.clickhouse.internal.options.ClickHouseConnectionOptions;
import org.apache.flink.connector.clickhouse.internal.schema.ClusterSpec;
import org.apache.flink.connector.clickhouse.internal.schema.ShardSpec;

import com.clickhouse.client.config.ClickHouseDefaults;
import com.clickhouse.jdbc.ClickHouseConnection;
import com.clickhouse.jdbc.ClickHouseDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.util.stream.Collectors.toList;
import static org.apache.flink.connector.clickhouse.util.ClickHouseJdbcUtil.getClusterSpec;

/** ClickHouse connection provider. Use ClickHouseDriver to create a connection. */
public class ClickHouseConnectionProvider implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseConnectionProvider.class);

    private final ClickHouseConnectionOptions options;

    private final Properties connectionProperties;

    private transient ClickHouseConnection connection;

    private transient List<ClickHouseConnection> shardConnections;

    public ClickHouseConnectionProvider(ClickHouseConnectionOptions options) {
        this(options, new Properties());
    }

    public ClickHouseConnectionProvider(
            ClickHouseConnectionOptions options, Properties connectionProperties) {
        this.options = options;
        this.connectionProperties = connectionProperties;
    }

    public boolean isConnectionValid() throws SQLException {
        return connection != null;
    }

    public synchronized ClickHouseConnection getOrCreateConnection() throws SQLException {
        if (connection == null) {
            connection = createConnection(options.getUrl());
        }
        return connection;
    }

    public synchronized Map<Integer, ClickHouseConnection> createShardConnections(
            ClusterSpec clusterSpec) throws SQLException {
        Map<Integer, ClickHouseConnection> connectionMap = new HashMap<>();
        String urlSuffix = options.getUrlSuffix();
        for (ShardSpec shardSpec : clusterSpec.getShards()) {
            String shardUrl = shardSpec.getJdbcUrls() + urlSuffix;
            ClickHouseConnection connection = createAndStoreShardConnection(shardUrl);
            connectionMap.put(shardSpec.getNum(), connection);
        }

        return connectionMap;
    }

    public synchronized ClickHouseConnection createAndStoreShardConnection(String url)
            throws SQLException {
        if (shardConnections == null) {
            shardConnections = new ArrayList<>();
        }

        ClickHouseConnection connection = createConnection(url);
        shardConnections.add(connection);
        return connection;
    }

    public List<String> getShardUrls(String remoteCluster) throws SQLException {
        Map<Integer, String> shardsMap = new HashMap<>();
        ClickHouseConnection conn = getOrCreateConnection();
        ClusterSpec clusterSpec = getClusterSpec(conn, remoteCluster);
        String urlSuffix = options.getUrlSuffix();
        for (ShardSpec shardSpec : clusterSpec.getShards()) {
            String shardUrl = shardSpec.getJdbcUrls() + urlSuffix;
            shardsMap.put(shardSpec.getNum(), shardUrl);
        }

        return shardsMap.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(Map.Entry::getValue)
                .collect(toList());
    }

    private ClickHouseConnection createConnection(String url) throws SQLException {
        LOG.info("connecting to {}", url);
        Properties configuration = new Properties();
        configuration.putAll(connectionProperties);
        if (options.getUsername().isPresent()) {
            configuration.setProperty(
                    ClickHouseDefaults.USER.getKey(), options.getUsername().get());
        }
        if (options.getPassword().isPresent()) {
            configuration.setProperty(
                    ClickHouseDefaults.PASSWORD.getKey(), options.getPassword().get());
        }
        ClickHouseDriver driver = new ClickHouseDriver();
        return driver.connect(url, configuration);
    }

    public void closeConnections() {
        if (this.connection != null) {
            try {
                connection.close();
            } catch (SQLException exception) {
                LOG.warn("ClickHouse connection could not be closed.", exception);
            } finally {
                connection = null;
            }
        }

        if (shardConnections != null) {
            for (ClickHouseConnection shardConnection : this.shardConnections) {
                try {
                    shardConnection.close();
                } catch (SQLException exception) {
                    LOG.warn("ClickHouse shard connection could not be closed.", exception);
                }
            }

            shardConnections = null;
        }
    }
}
