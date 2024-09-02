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

package org.apache.flink.connector.clickhouse.internal.converter;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;

import com.clickhouse.data.value.UnsignedByte;
import com.clickhouse.data.value.UnsignedInteger;
import com.clickhouse.data.value.UnsignedLong;
import com.clickhouse.data.value.UnsignedShort;

import org.apache.flink.table.types.logical.RowType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.sql.Array;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.flink.connector.clickhouse.util.ClickHouseUtil.getFlinkTimeZone;
import static org.apache.flink.connector.clickhouse.util.ClickHouseUtil.toEpochDayOneTimestamp;

/** convert between internal and external data types. */
public class ClickHouseConverterUtils {

    public static final int BOOL_TRUE = 1;

    public static Object toExternal(Object value, LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
            case BIGINT:
            case INTERVAL_DAY_TIME:
            case FLOAT:
            case DOUBLE:
            case BINARY:
            case VARBINARY:
                return value;
            case CHAR:
            case VARCHAR:
                return value.toString();
            case DATE:
                return Date.valueOf(LocalDate.ofEpochDay((Integer) value));
            case TIME_WITHOUT_TIME_ZONE:
                LocalTime localTime = LocalTime.ofNanoOfDay(((Integer) value) * 1_000_000L);
                return toEpochDayOneTimestamp(localTime);
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return ((TimestampData) value).toTimestamp();
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return Timestamp.from(((TimestampData) value).toInstant());
            case DECIMAL:
                return ((DecimalData) value).toBigDecimal();
            case ARRAY:
                LogicalType elementType =
                        ((ArrayType) type)
                                .getChildren().stream()
                                        .findFirst()
                                        .orElseThrow(
                                                () ->
                                                        new RuntimeException(
                                                                "Unknown array element type"));
                ArrayData.ElementGetter elementGetter = ArrayData.createElementGetter(elementType);
                ArrayData arrayData = ((ArrayData) value);
                Object[] objectArray = new Object[arrayData.size()];
                for (int i = 0; i < arrayData.size(); i++) {
                    objectArray[i] =
                            toExternal(elementGetter.getElementOrNull(arrayData, i), elementType);
                }
                return objectArray;
            case MAP:
                LogicalType keyType = ((MapType) type).getKeyType();
                LogicalType valueType = ((MapType) type).getValueType();
                ArrayData.ElementGetter keyGetter = ArrayData.createElementGetter(keyType);
                ArrayData.ElementGetter valueGetter = ArrayData.createElementGetter(valueType);
                MapData mapData = (MapData) value;
                ArrayData keyArrayData = mapData.keyArray();
                ArrayData valueArrayData = mapData.valueArray();
                Map<Object, Object> objectMap = new HashMap<>(keyArrayData.size());
                for (int i = 0; i < keyArrayData.size(); i++) {
                    objectMap.put(
                            toExternal(keyGetter.getElementOrNull(keyArrayData, i), keyType),
                            toExternal(valueGetter.getElementOrNull(valueArrayData, i), valueType));
                }
                return objectMap;
            case ROW:
                List<Object> result = new ArrayList<>();
                for (int i = 0; i < ((RowData) value).getArity(); i++) {
                    result.add(
                            toExternal(
                                    RowData.createFieldGetter(
                                                    ((RowType) type).getTypeAt(i), i)
                                            .getFieldOrNull((RowData) value),
                                    ((RowType) type).getTypeAt(i)));
                }
                return result;
            case RAW:
            case MULTISET:
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    public static Object toInternal(Object value, LogicalType type) throws SQLException {
        switch (type.getTypeRoot()) {
            case NULL:
                return null;
            case BOOLEAN:
                return value instanceof Number ? BOOL_TRUE == ((Number) value).intValue() : value;
            case FLOAT:
            case DOUBLE:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY_TIME:
            case TINYINT:
            case BINARY:
            case VARBINARY:
                return value;
            case SMALLINT:
                return value instanceof UnsignedByte ? ((UnsignedByte) value).shortValue() : value;
            case INTEGER:
                return value instanceof UnsignedShort ? ((UnsignedShort) value).intValue() : value;
            case BIGINT:
                return value instanceof UnsignedInteger
                        ? ((UnsignedInteger) value).longValue()
                        : value;
            case DECIMAL:
                final int precision = ((DecimalType) type).getPrecision();
                final int scale = ((DecimalType) type).getScale();
                BigDecimal decimalValue =
                        value instanceof BigDecimal
                                ? (BigDecimal) value
                                : new BigDecimal(
                                        value instanceof UnsignedLong
                                                ? ((UnsignedLong) value).bigIntegerValue()
                                                : (BigInteger) value);
                return DecimalData.fromBigDecimal(decimalValue, precision, scale);
            case DATE:
                return (int) (((LocalDate) value).toEpochDay());
            case TIME_WITHOUT_TIME_ZONE:
                return (int) (((Time) value).toLocalTime().toNanoOfDay() / 1_000_000L);
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return TimestampData.fromLocalDateTime(
                        value instanceof OffsetDateTime
                                ? ((OffsetDateTime) value).toLocalDateTime()
                                : (LocalDateTime) value);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return TimestampData.fromInstant(
                        ((LocalDateTime) value).atZone(getFlinkTimeZone().toZoneId()).toInstant());
            case CHAR:
            case VARCHAR:
                if (value instanceof UUID) {
                    return StringData.fromString(value.toString());
                } else if (value instanceof InetAddress) {
                    return StringData.fromString(((InetAddress) value).getHostAddress());
                } else {
                    return StringData.fromString((String) value);
                }
            case ARRAY:
                LogicalType elementType =
                        type.getChildren().stream()
                                .findFirst()
                                .orElseThrow(
                                        () -> new RuntimeException("Unknown array element type"));
                Object externalArray =
                        value.getClass().isArray() ? value : ((Array) value).getArray();
                int externalArrayLength = java.lang.reflect.Array.getLength(externalArray);
                Object[] internalArray = new Object[externalArrayLength];
                for (int i = 0; i < externalArrayLength; i++) {
                    internalArray[i] =
                            toInternal(java.lang.reflect.Array.get(externalArray, i), elementType);
                }
                return new GenericArrayData(internalArray);
            case MAP:
                LogicalType keyType = ((MapType) type).getKeyType();
                LogicalType valueType = ((MapType) type).getValueType();
                Map<?, ?> externalMap = (Map<?, ?>) value;
                Map<Object, Object> internalMap = new HashMap<>(externalMap.size());
                for (Map.Entry<?, ?> entry : externalMap.entrySet()) {
                    internalMap.put(
                            toInternal(entry.getKey(), keyType),
                            toInternal(entry.getValue(), valueType));
                }
                return new GenericMapData(internalMap);
            case ROW:
                List<Object> row = (List<Object>) value;
                GenericRowData rowData = new GenericRowData(row.size());
                for (int i = 0; i < row.size(); i++) {
                    rowData.setField(i, toInternal(row.get(i), type.getChildren().get(i)));
                }
                return rowData;
            case MULTISET:
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }
}
