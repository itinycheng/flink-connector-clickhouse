//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package com.tiny.flink.connector.clickhouse.internal.connection;

import com.tiny.flink.connector.clickhouse.internal.options.ClickHouseOptions;
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

    private transient ClickHouseConnection connection;

    private transient List<ClickHouseConnection> shardConnections;

    private final ClickHouseOptions options;

    public ClickHouseConnectionProvider(ClickHouseOptions options) {
        this.options = options;
    }

    public synchronized ClickHouseConnection getConnection() throws SQLException {
        if (this.connection == null) {
            this.connection =
                    createConnection(this.options.getUrl(), this.options.getDatabaseName());
        }

        return this.connection;
    }

    private ClickHouseConnection createConnection(String url, String database) throws SQLException {
        LOG.info("connecting to {}", url);

        try {
            Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        } catch (ClassNotFoundException var4) {
            throw new SQLException(var4);
        }

        ClickHouseConnection conn;
        if (this.options.getUsername().isPresent()) {
            conn =
                    (ClickHouseConnection)
                            DriverManager.getConnection(
                                    this.getJdbcUrl(url, database),
                                    this.options.getUsername().orElse(null),
                                    this.options.getPassword().orElse(null));
        } else {
            conn =
                    (ClickHouseConnection)
                            DriverManager.getConnection(this.getJdbcUrl(url, database));
        }

        return conn;
    }

    public synchronized List<ClickHouseConnection> getShardConnections(
            String remoteCluster, String remoteDatabase) throws SQLException {
        if (this.shardConnections == null) {
            ClickHouseConnection conn = this.getConnection();
            PreparedStatement stmt =
                    conn.prepareStatement(
                            "SELECT shard_num, host_address, port FROM system.clusters WHERE cluster = ?");
            stmt.setString(1, remoteCluster);
            ResultSet rs = stmt.executeQuery();
            Throwable var6 = null;

            try {
                this.shardConnections = new ArrayList<>();

                while (rs.next()) {
                    String host = rs.getString("host_address");
                    int port = this.getActualHttpPort(host, rs.getInt("port"));
                    String url = "clickhouse://" + host + ":" + port;
                    this.shardConnections.add(this.createConnection(url, remoteDatabase));
                }
            } catch (Throwable var17) {
                var6 = var17;
                throw var17;
            } finally {
                if (rs != null) {
                    if (var6 != null) {
                        try {
                            rs.close();
                        } catch (Throwable var16) {
                            var6.addSuppressed(var16);
                        }
                    } else {
                        rs.close();
                    }
                }
            }

            if (this.shardConnections.isEmpty()) {
                throw new SQLException("unable to query shards in system.clusters");
            }
        }

        return this.shardConnections;
    }

    private int getActualHttpPort(String host, int port) throws SQLException {
        try {
            CloseableHttpClient httpclient = HttpClients.createDefault();
            Throwable var4 = null;

            int var8;
            try {
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

                    throw new SQLException("Cannot query ClickHouse http port");
                }

                var8 = port;
            } catch (Throwable var21) {
                var4 = var21;
                throw var21;
            } finally {
                if (httpclient != null) {
                    if (var4 != null) {
                        try {
                            httpclient.close();
                        } catch (Throwable var20) {
                            var4.addSuppressed(var20);
                        }
                    } else {
                        httpclient.close();
                    }
                }
            }

            return var8;
        } catch (Exception var23) {
            throw new SQLException("Cannot connect to ClickHouse server using HTTP", var23);
        }
    }

    public void closeConnections() throws SQLException {
        if (this.connection != null) {
            this.connection.close();
        }

        if (this.shardConnections != null) {
            for (ClickHouseConnection shardConnection : this.shardConnections) {
                shardConnection.close();
            }
        }
    }

    private String getJdbcUrl(String url, String database) throws SQLException {
        try {
            return "jdbc:" + (new URIBuilder(url)).setPath("/" + database).build().toString();
        } catch (Exception var4) {
            throw new SQLException(var4);
        }
    }

    public String queryTableEngine(String databaseName, String tableName) throws SQLException {
        ClickHouseConnection conn = this.getConnection();
        PreparedStatement stmt =
                conn.prepareStatement(
                        "SELECT engine_full FROM system.tables WHERE database = ? AND name = ?");
        Throwable var5 = null;

        try {
            stmt.setString(1, databaseName);
            stmt.setString(2, tableName);
            ResultSet rs = stmt.executeQuery();
            Throwable var7 = null;

            try {
                String var8;
                try {
                    if (rs.next()) {
                        var8 = rs.getString("engine_full");
                        return var8;
                    }
                } catch (Throwable var34) {
                    var7 = var34;
                    throw var34;
                }
            } finally {
                if (rs != null) {
                    if (var7 != null) {
                        try {
                            rs.close();
                        } catch (Throwable var33) {
                            var7.addSuppressed(var33);
                        }
                    } else {
                        rs.close();
                    }
                }
            }
        } catch (Throwable var36) {
            var5 = var36;
            throw var36;
        } finally {
            if (stmt != null) {
                if (var5 != null) {
                    try {
                        stmt.close();
                    } catch (Throwable var32) {
                        var5.addSuppressed(var32);
                    }
                } else {
                    stmt.close();
                }
            }
        }

        throw new SQLException("table `" + databaseName + "`.`" + tableName + "` does not exist");
    }
}
