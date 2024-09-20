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

package org.apache.flink.connector.clickhouse.internal.options;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.connector.clickhouse.util.ClickHouseUtil.EMPTY;
import static org.apache.flink.util.StringUtils.isNullOrWhitespaceOnly;

/** ClickHouse connection options. */
public class ClickHouseConnectionOptions implements Cloneable, Serializable {

    private static final long serialVersionUID = 1L;

    public static final Pattern URL_PATTERN =
            Pattern.compile("[^/]+//[^/?]+(/(?<database>[^?]*))?(\\?(?<param>\\S+))?");

    private final String url;

    private final String username;

    private final String password;

    private final String databaseName;

    private final String tableName;

    // For testing.
    @VisibleForTesting
    public ClickHouseConnectionOptions(String url) {
        this(url, null, null, null, null);
    }

    protected ClickHouseConnectionOptions(
            String url,
            @Nullable String username,
            @Nullable String password,
            String databaseName,
            String tableName) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    /**
     * The format of the URL suffix is as follows: {@code
     * [/<database>][?param1=value1&param2=value2]}.
     */
    public String getUrlSuffix() {
        Matcher matcher = URL_PATTERN.matcher(url);
        if (!matcher.find()) {
            return EMPTY;
        }

        String database = matcher.group("database");
        String param = matcher.group("param");
        database = isNullOrWhitespaceOnly(database) ? EMPTY : "/" + database;
        param = isNullOrWhitespaceOnly(param) ? EMPTY : "?" + param;
        return database + param;
    }

    public String getUrl() {
        return this.url;
    }

    public Optional<String> getUsername() {
        return Optional.ofNullable(this.username);
    }

    public Optional<String> getPassword() {
        return Optional.ofNullable(this.password);
    }

    public String getDatabaseName() {
        return this.databaseName;
    }

    public String getTableName() {
        return this.tableName;
    }

    @Override
    public ClickHouseConnectionOptions clone() {
        try {
            return (ClickHouseConnectionOptions) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new FlinkRuntimeException(e);
        }
    }
}
