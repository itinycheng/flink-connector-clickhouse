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

import org.apache.flink.util.Preconditions;

import java.sql.Array;
import java.sql.ResultSet;
import java.util.Map;

/** Wrap object array. */
public class ObjectArray implements Array {

    private Object[] array;

    public ObjectArray(Object[] array) {
        this.array = Preconditions.checkNotNull(array);
    }

    @Override
    public String getBaseTypeName() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getBaseType() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getArray() {
        return array;
    }

    @Override
    public Object getArray(Map<String, Class<?>> map) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getArray(long index, int count) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getArray(long index, int count, Map<String, Class<?>> map) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResultSet getResultSet() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResultSet getResultSet(Map<String, Class<?>> map) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResultSet getResultSet(long index, int count) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void free() {
        this.array = null;
    }
}
