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

import org.apache.flink.connector.clickhouse.internal.schema.ClusterSpec;
import org.apache.flink.connector.clickhouse.internal.schema.DistributedEngineFull;
import org.apache.flink.connector.clickhouse.internal.schema.ReplicaSpec;
import org.apache.flink.connector.clickhouse.internal.schema.ShardSpec;
import org.apache.flink.util.Preconditions;

import org.apache.hc.client5.http.HttpResponseException;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.net.URIBuilder;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static org.apache.flink.connector.clickhouse.util.ClickHouseUtil.parseShardingKey;

/** ClickHouse jdbc util. */
public class ClickHouseJdbcUtil {

    public static final Pattern DISTRIBUTED_TABLE_ENGINE_PATTERN =
            Pattern.compile(
                    "Distributed\\((?<cluster>[a-zA-Z_]\\w*),(?<database>[a-zA-Z_]\\w*),(?<table>[a-zA-Z_]\\w*)(,(?<shardingKey>[a-zA-Z_]\\w*\\(.*\\)|[a-zA-Z_]\\w*)?.*)?\\)");

    private static final String QUERY_TABLE_ENGINE_SQL =
            "SELECT engine_full FROM system.tables WHERE database = ? AND name = ?";

    private static final Pattern HTTP_PORT_PATTERN =
            Pattern.compile("You must use port (?<port>[0-9]+) for HTTP.");

    private static final String QUERY_CLUSTER_INFO_SQL =
            "SELECT cluster, shard_num, shard_weight, replica_num, host_address, port FROM system.clusters WHERE cluster = ?";

    public static DistributedEngineFull getDistributedEngineFull(
            Connection connection, String databaseName, String tableName) throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement(QUERY_TABLE_ENGINE_SQL)) {
            stmt.setString(1, databaseName);
            stmt.setString(2, tableName);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    String engineFull = rs.getString("engine_full").replaceAll("'|\\s", "");
                    Matcher matcher = DISTRIBUTED_TABLE_ENGINE_PATTERN.matcher(engineFull);
                    if (matcher.find()) {
                        String cluster = matcher.group("cluster");
                        String database = matcher.group("database");
                        String table = matcher.group("table");
                        String shardingKey = matcher.group("shardingKey");
                        return DistributedEngineFull.of(
                                cluster, database, table, parseShardingKey(shardingKey), null);
                    } else {
                        return null;
                    }
                }
            }
        }

        throw new SQLException(
                String.format("table `%s`.`%s` does not exist", databaseName, tableName));
    }

    public static ClusterSpec getClusterSpec(Connection connection, String clusterName)
            throws SQLException {
        List<ClusterRow> clusterRows = new ArrayList<>();
        try (PreparedStatement stmt = connection.prepareStatement(QUERY_CLUSTER_INFO_SQL)) {
            stmt.setString(1, clusterName);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    clusterRows.add(
                            new ClusterRow(
                                    rs.getString("cluster"),
                                    rs.getInt("shard_num"),
                                    rs.getLong("shard_weight"),
                                    rs.getInt("replica_num"),
                                    rs.getString("host_address"),
                                    rs.getInt("port")));
                }
            }
        }

        List<ShardSpec> shardSpecs =
                clusterRows.stream()
                        .collect(groupingBy(clusterRow -> clusterRow.shardNum))
                        .entrySet()
                        .stream()
                        .map(
                                shardEntry -> {
                                    List<ClusterRow> replicas = shardEntry.getValue();
                                    Set<Long> weights = new HashSet<>(replicas.size());
                                    List<ReplicaSpec> replicaSpecs =
                                            replicas.stream()
                                                    .peek(
                                                            clusterRow ->
                                                                    weights.add(
                                                                            clusterRow.shardWeight))
                                                    .map(
                                                            clusterRow -> {
                                                                try {
                                                                    int actualHttpPort =
                                                                            getActualHttpPort(
                                                                                    clusterRow
                                                                                            .hostAddress,
                                                                                    clusterRow
                                                                                            .port);
                                                                    return new ReplicaSpec(
                                                                            clusterRow.replicaNum,
                                                                            clusterRow.hostAddress,
                                                                            actualHttpPort);
                                                                } catch (Exception e) {
                                                                    throw new RuntimeException(e);
                                                                }
                                                            })
                                                    .collect(toList());

                                    Preconditions.checkState(
                                            weights.size() == 1,
                                            "Weights of the same shard must be equal");
                                    return new ShardSpec(
                                            shardEntry.getKey(),
                                            weights.stream().findAny().get(),
                                            replicaSpecs);
                                })
                        .collect(toList());
        return new ClusterSpec(clusterName, shardSpecs);
    }

    public static int getActualHttpPort(String host, int port) throws SQLException {
        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
            HttpUriRequestBase request =
                    new HttpGet(
                            (new URIBuilder())
                                    .setScheme("http")
                                    .setHost(host)
                                    .setPort(port)
                                    .build());

            try (CloseableHttpResponse response = httpclient.execute(request)) {
                int statusCode = response.getCode();

                if (statusCode != 200) {
                    String raw = EntityUtils.toString(response.getEntity());
                    Matcher matcher = HTTP_PORT_PATTERN.matcher(raw);
                    if (matcher.find()) {
                        return Integer.parseInt(matcher.group("port"));
                    }
                    throw new SQLException("Cannot query ClickHouse http port.");
                }
                return port;
            } catch (ParseException | HttpResponseException e) {
                throw new SQLException("Error parsing response.", e);
            }
        } catch (Throwable throwable) {
            throw new SQLException("Cannot connect to ClickHouse server using HTTP.", throwable);
        }
    }

    private static class ClusterRow {
        public final String cluster;
        public final Integer shardNum;
        public final Long shardWeight;
        public final Integer replicaNum;
        public final String hostAddress;
        public final Integer port;

        public ClusterRow(
                String cluster,
                Integer shardNum,
                Long shardWeight,
                Integer replicaNum,
                String hostAddress,
                Integer port) {
            this.cluster = cluster;
            this.shardNum = shardNum;
            this.shardWeight = shardWeight;
            this.replicaNum = replicaNum;
            this.hostAddress = hostAddress;
            this.port = port;
        }
    }
}
