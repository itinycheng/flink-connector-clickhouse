package org.apache.flink.connector.clickhouse.internal.connection;

import org.apache.flink.connector.clickhouse.internal.options.ClickHouseConnectionOptions;
import org.apache.flink.connector.clickhouse.internal.schema.ClusterSpec;
import org.apache.flink.connector.clickhouse.internal.schema.ShardSpec;
import org.apache.flink.connector.clickhouse.util.ClickHouseUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.BalancedClickhouseDataSource;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.util.stream.Collectors.toList;
import static org.apache.flink.connector.clickhouse.util.ClickHouseJdbcUtil.getActualHttpPort;

/** ClickHouse connection provider. Use ClickHouseDriver to create a connection. */
public class ClickHouseConnectionProvider implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseConnectionProvider.class);

    private static final String QUERY_CLUSTER_INFO_SQL =
            "SELECT shard_num, host_address, port FROM system.clusters WHERE cluster = ? ORDER BY shard_num, replica_num ASC";

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
            connection = createConnection(options.getUrl(), options.getDatabaseName());
        }
        return connection;
    }

    public synchronized Map<Integer, ClickHouseConnection> createShardConnections(
            ClusterSpec clusterSpec, String defaultDatabase) throws SQLException {
        Map<Integer, ClickHouseConnection> connectionMap = new HashMap<>();
        for (ShardSpec shardSpec : clusterSpec.getShards()) {
            ClickHouseConnection connection =
                    createAndStoreShardConnection(shardSpec.getJdbcUrls(), defaultDatabase);
            connectionMap.put(shardSpec.getNum(), connection);
        }

        return connectionMap;
    }

    public synchronized ClickHouseConnection createAndStoreShardConnection(
            String url, String database) throws SQLException {
        if (shardConnections == null) {
            shardConnections = new ArrayList<>();
        }

        ClickHouseConnection connection = createConnection(url, database);
        shardConnections.add(connection);
        return connection;
    }

    public List<String> getShardUrls(String remoteCluster) throws SQLException {
        Map<Long, List<String>> shardsMap = new HashMap<>();
        ClickHouseConnection conn = getOrCreateConnection();
        try (PreparedStatement stmt = conn.prepareStatement(QUERY_CLUSTER_INFO_SQL)) {
            stmt.setString(1, remoteCluster);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String host = rs.getString("host_address");
                    int port = getActualHttpPort(host, rs.getInt("port"));
                    List<String> shardUrls =
                            shardsMap.computeIfAbsent(
                                    rs.getLong("shard_num"), k -> new ArrayList<>());
                    shardUrls.add(host + ":" + port);
                }
            }
        }

        return shardsMap.values().stream()
                .map(urls -> "clickhouse://" + String.join(",", urls))
                .collect(toList());
    }

    private ClickHouseConnection createConnection(String url, String database) throws SQLException {
        LOG.info("connecting to {}, database {}", url, database);

        String jdbcUrl = ClickHouseUtil.getJdbcUrl(url, database);
        ClickHouseProperties properties = new ClickHouseProperties(connectionProperties);
        properties.setUser(options.getUsername().orElse(null));
        properties.setPassword(options.getPassword().orElse(null));
        BalancedClickhouseDataSource dataSource =
                new BalancedClickhouseDataSource(jdbcUrl, properties);
        if (dataSource.getAllClickhouseUrls().size() > 1) {
            dataSource.actualize();
        }
        return dataSource.getConnection();
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
