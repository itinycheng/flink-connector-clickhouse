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
