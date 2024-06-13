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

package org.apache.flink.connector.clickhouse.internal.schema;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.StringUtils.isNullOrWhitespaceOnly;

/** Distributed table engine full schema. */
public class DistributedEngineFull implements Serializable {

    private final String cluster;

    private final String database;

    private final String table;

    private final Expression shardingKey;

    private final String policyName;

    public static DistributedEngineFull of(
            String cluster,
            String database,
            String table,
            Expression shardingKey,
            String policyName) {
        return new DistributedEngineFull(cluster, database, table, shardingKey, policyName);
    }

    private DistributedEngineFull(
            String cluster,
            String database,
            String table,
            Expression shardingKey,
            String policyName) {
        checkArgument(!isNullOrWhitespaceOnly(cluster), "cluster cannot be null or empty");
        checkArgument(!isNullOrWhitespaceOnly(database), "database cannot be null or empty");
        checkArgument(!isNullOrWhitespaceOnly(table), "table cannot be null or empty");

        this.cluster = cluster;
        this.database = database;
        this.table = table;
        this.shardingKey = shardingKey;
        this.policyName = policyName;
    }

    public String getCluster() {
        return cluster;
    }

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    public Expression getShardingKey() {
        return shardingKey;
    }

    public String getPolicyName() {
        return policyName;
    }
}
