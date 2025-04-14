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

package org.apache.flink.connector.clickhouse.internal.executor;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SinkUpdateStrategy;
import org.apache.flink.connector.clickhouse.internal.connection.ClickHouseConnectionProvider;
import org.apache.flink.connector.clickhouse.internal.converter.ClickHouseRowConverter;
import org.apache.flink.connector.clickhouse.internal.options.ClickHouseDmlOptions;
import org.apache.flink.table.data.RowData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SinkUpdateStrategy.DISCARD;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SinkUpdateStrategy.INSERT;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SinkUpdateStrategy.UPDATE;

/** ClickHouse's upsert executor. */
public class ClickHouseUpsertExecutor implements ClickHouseExecutor {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseUpsertExecutor.class);

    private final String insertSql;

    private final String updateSql;

    private final String deleteSql;

    private final ClickHouseRowConverter insertConverter;

    private final ClickHouseRowConverter updateConverter;

    private final ClickHouseRowConverter deleteConverter;

    private final Function<RowData, RowData> updateExtractor;

    private final Function<RowData, RowData> keyExtractor;

    private final int maxRetries;

    private final SinkUpdateStrategy updateStrategy;

    private final boolean ignoreDelete;

    private final Map<RowData, RowData> reduceBuffer = new HashMap<>();

    private transient PreparedStatement insertStatement;

    private transient PreparedStatement updateStatement;

    private transient PreparedStatement deleteStatement;

    private transient ClickHouseConnectionProvider connectionProvider;

    public ClickHouseUpsertExecutor(
            String insertSql,
            String updateSql,
            String deleteSql,
            ClickHouseRowConverter insertConverter,
            ClickHouseRowConverter updateConverter,
            ClickHouseRowConverter deleteConverter,
            Function<RowData, RowData> updateExtractor,
            Function<RowData, RowData> keyExtractor,
            ClickHouseDmlOptions options) {
        this.insertSql = insertSql;
        this.updateSql = updateSql;
        this.deleteSql = deleteSql;
        this.insertConverter = insertConverter;
        this.updateConverter = updateConverter;
        this.deleteConverter = deleteConverter;
        this.updateExtractor = updateExtractor;
        this.keyExtractor = keyExtractor;
        this.maxRetries = options.getMaxRetries();
        this.updateStrategy = options.getUpdateStrategy();
        this.ignoreDelete = options.isIgnoreDelete();
    }

    @Override
    public void prepareStatement(Connection connection) throws SQLException {
        this.insertStatement = connection.prepareStatement(this.insertSql);
        this.updateStatement = connection.prepareStatement(this.updateSql);
        this.deleteStatement = connection.prepareStatement(this.deleteSql);
    }

    @Override
    public void prepareStatement(ClickHouseConnectionProvider connectionProvider)
            throws SQLException {
        this.connectionProvider = connectionProvider;
        prepareStatement(connectionProvider.getOrCreateConnection());
    }

    @Override
    public void setRuntimeContext(RuntimeContext context) {}

    @Override
    public void addToBatch(RowData record) {
        RowData key = keyExtractor.apply(record);
        reduceBuffer.put(key, record);
    }

    @Override
    public void executeBatch() throws SQLException {
        for (RowData value : reduceBuffer.values()) {
            addValueToBatch(value);
        }

        for (PreparedStatement clickHouseStatement :
                Arrays.asList(insertStatement, updateStatement, deleteStatement)) {
            if (clickHouseStatement != null) {
                attemptExecuteBatch(clickHouseStatement, maxRetries);
            }
        }

        reduceBuffer.clear();
    }

    private void addValueToBatch(RowData record) throws SQLException {
        switch (record.getRowKind()) {
            case INSERT:
                insertConverter.toExternal(record, insertStatement);
                insertStatement.addBatch();
                break;
            case UPDATE_AFTER:
                if (INSERT.equals(updateStrategy)) {
                    insertConverter.toExternal(record, insertStatement);
                    insertStatement.addBatch();
                } else if (UPDATE.equals(updateStrategy)) {
                    updateConverter.toExternal(updateExtractor.apply(record), updateStatement);
                    updateStatement.addBatch();
                } else if (DISCARD.equals(updateStrategy)) {
                    LOG.debug("Discard a record of type UPDATE_AFTER: {}", record);
                } else {
                    throw new RuntimeException("Unknown update strategy: " + updateStrategy);
                }
                break;
            case DELETE:
                if (!ignoreDelete) {
                    deleteConverter.toExternal(keyExtractor.apply(record), deleteStatement);
                    deleteStatement.addBatch();
                }
                break;
            case UPDATE_BEFORE:
                break;
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Unknown row kind, the supported row kinds is: INSERT, UPDATE_BEFORE, UPDATE_AFTER, DELETE, but get: %s.",
                                record.getRowKind()));
        }
    }

    @Override
    public void closeStatement() {
        for (PreparedStatement clickHouseStatement :
                Arrays.asList(insertStatement, updateStatement, deleteStatement)) {
            if (clickHouseStatement != null) {
                try {
                    clickHouseStatement.close();
                } catch (SQLException exception) {
                    LOG.warn("ClickHouse upsert statement could not be closed.", exception);
                }
            }
        }
    }

    @Override
    public String toString() {
        return "ClickHouseUpsertExecutor{"
                + "insertSql='"
                + insertSql
                + '\''
                + ", updateSql='"
                + updateSql
                + '\''
                + ", deleteSql='"
                + deleteSql
                + '\''
                + ", maxRetries="
                + maxRetries
                + ", updateStrategy="
                + updateStrategy
                + ", ignoreDelete="
                + ignoreDelete
                + ", connectionProvider="
                + connectionProvider
                + '}';
    }
}
