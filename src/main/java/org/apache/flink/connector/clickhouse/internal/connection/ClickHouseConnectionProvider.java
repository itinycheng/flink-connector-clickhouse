//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.flink.connector.clickhouse.internal.connection;

import org.apache.flink.connector.clickhouse.internal.options.ClickHouseOptions;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.ClickHouseConnection;

import java.io.Serializable;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** ClickHouse connection provider. Use ClickHouseDriver to create a connection. */
public class ClickHouseConnectionProvider implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseConnectionProvider.class);

    private static final String CLICKHOUSE_DRIVER_NAME = "ru.yandex.clickhouse.ClickHouseDriver";

    private static final Pattern PATTERN =
            Pattern.compile("You must use port (?<port>[0-9]+) for HTTP.");

    private static final String QUERY_CLUSTER_INFO_SQL =
            "SELECT shard_num, host_address, port FROM system.clusters WHERE cluster = ?";

    private static final String QUERY_TABLE_ENGINE_SQL =
            "SELECT engine_full FROM system.tables WHERE database = ? AND name = ?";

    private transient ClickHouseConnection connection;

    private transient List<ClickHouseConnection> shardConnections;

    private final ClickHouseOptions options;

    public ClickHouseConnectionProvider(ClickHouseOptions options) {
        this.options = options;
    }

    public synchronized ClickHouseConnection getConnection() throws SQLException {
        if (connection == null) {
            connection = createConnection(options.getUrl(), options.getDatabaseName());
        }
        return connection;
    }

    public synchronized List<ClickHouseConnection> getShardConnections(
            String remoteCluster, String remoteDatabase) throws SQLException {
        if (shardConnections == null) {
            try (ClickHouseConnection conn = getConnection();
                    PreparedStatement stmt = conn.prepareStatement(QUERY_CLUSTER_INFO_SQL)) {
                stmt.setString(1, remoteCluster);
                try (ResultSet rs = stmt.executeQuery()) {
                    shardConnections = new ArrayList<>();
                    while (rs.next()) {
                        String host = rs.getString("host_address");
                        int port = getActualHttpPort(host, rs.getInt("port"));
                        String url = "clickhouse://" + host + ":" + port;
                        shardConnections.add(createConnection(url, remoteDatabase));
                    }
                }
            }

            if (shardConnections.isEmpty()) {
                throw new SQLException("unable to query shards in system.clusters");
            }
        }

        return shardConnections;
    }

    private int getActualHttpPort(String host, int port) throws SQLException {
        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
            HttpGet request =
                    new HttpGet(
                            (new URIBuilder())
                                    .setScheme("http")
                                    .setHost(host)
                                    .setPort(port)
                                    .build());
            HttpResponse response = httpclient.execute(request);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != 200) {
                String raw = EntityUtils.toString(response.getEntity());
                Matcher matcher = PATTERN.matcher(raw);
                if (matcher.find()) {
                    return Integer.parseInt(matcher.group("port"));
                }
                throw new SQLException("Cannot query ClickHouse http port.");
            }

            return port;
        } catch (Throwable throwable) {
            throw new SQLException("Cannot connect to ClickHouse server using HTTP.", throwable);
        }
    }

    public void closeConnections() throws SQLException {
        if (this.connection != null) {
            connection.close();
        }

        if (shardConnections != null) {
            for (ClickHouseConnection shardConnection : this.shardConnections) {
                shardConnection.close();
            }
        }
    }

    private String getJdbcUrl(String url, String database) throws SQLException {
        try {
            return "jdbc:" + (new URIBuilder(url)).setPath("/" + database).build().toString();
        } catch (Exception exception) {
            throw new SQLException(exception);
        }
    }

    public String queryTableEngine(String databaseName, String tableName) throws SQLException {
        try (ClickHouseConnection conn = getConnection();
                PreparedStatement stmt = conn.prepareStatement(QUERY_TABLE_ENGINE_SQL)) {
            stmt.setString(1, databaseName);
            stmt.setString(2, tableName);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getString("engine_full");
                }
            }
        }

        throw new SQLException(
                String.format("table `%s`.`%s` does not exist", databaseName, tableName));
    }

    private ClickHouseConnection createConnection(String url, String database) throws SQLException {
        LOG.info("connecting to {}", url);

        try {
            Class.forName(CLICKHOUSE_DRIVER_NAME);
        } catch (ClassNotFoundException exception) {
            throw new SQLException(exception);
        }

        return (ClickHouseConnection)
                DriverManager.getConnection(
                        getJdbcUrl(url, database),
                        options.getUsername().orElse(null),
                        options.getPassword().orElse(null));
    }
}
