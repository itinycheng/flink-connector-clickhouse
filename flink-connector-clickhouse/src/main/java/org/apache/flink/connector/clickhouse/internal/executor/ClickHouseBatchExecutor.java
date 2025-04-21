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
import org.apache.flink.connector.clickhouse.internal.connection.ClickHouseConnectionProvider;
import org.apache.flink.connector.clickhouse.internal.connection.ClickHouseStatementWrapper;
import org.apache.flink.connector.clickhouse.internal.converter.ClickHouseRowConverter;
import org.apache.flink.connector.clickhouse.internal.options.ClickHouseDmlOptions;
import org.apache.flink.table.data.RowData;

import com.clickhouse.jdbc.ClickHouseConnection;
import com.clickhouse.jdbc.ClickHousePreparedStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

/** ClickHouse's batch executor. */
public class ClickHouseBatchExecutor implements ClickHouseExecutor {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseBatchExecutor.class);

    private final String insertSql;

    private final ClickHouseRowConverter converter;

    private final int maxRetries;

    private transient ClickHouseStatementWrapper statement;

    private transient ClickHouseConnectionProvider connectionProvider;

    public ClickHouseBatchExecutor(
            String insertSql, ClickHouseRowConverter converter, ClickHouseDmlOptions options) {
        this.insertSql = insertSql;
        this.converter = converter;
        this.maxRetries = options.getMaxRetries();
    }

    @Override
    public void prepareStatement(ClickHouseConnection connection) throws SQLException {
        statement =
                new ClickHouseStatementWrapper(
                        (ClickHousePreparedStatement) connection.prepareStatement(insertSql));
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
    public void addToBatch(RowData record) throws SQLException {
        switch (record.getRowKind()) {
            case INSERT:
                converter.toExternal(record, statement);
                statement.addBatch();
                break;
            case UPDATE_AFTER:
            case DELETE:
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
    public void executeBatch() throws SQLException {
        attemptExecuteBatch(statement, maxRetries);
    }

    @Override
    public void closeStatement() {
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException exception) {
                LOG.warn("ClickHouse batch statement could not be closed.", exception);
            } finally {
                statement = null;
            }
        }
    }

    @Override
    public String toString() {
        return "ClickHouseBatchExecutor{"
                + "insertSql='"
                + insertSql
                + '\''
                + ", maxRetries="
                + maxRetries
                + ", connectionProvider="
                + connectionProvider
                + '}';
    }
}
