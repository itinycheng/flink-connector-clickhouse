package org.apache.flink.connector.clickhouse.internal.converter;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Array;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;

/** convert between internal and external data types. */
public class ClickHouseConverterUtils {

    private static final LocalDate DATE_PREFIX_OF_TIME = LocalDate.ofEpochDay(1);

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
                return toFixedDateTimestamp(localTime);
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
            case MULTISET:
            case ROW:
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    public static Object toInternal(Object value, LogicalType type) throws SQLException {
        switch (type.getTypeRoot()) {
            case NULL:
                return null;
            case BOOLEAN:
            case FLOAT:
            case DOUBLE:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY_TIME:
            case INTEGER:
            case BIGINT:
            case BINARY:
            case VARBINARY:
                return value;
            case TINYINT:
                return ((Integer) value).byteValue();
            case SMALLINT:
                return value instanceof Integer ? ((Integer) value).shortValue() : value;
            case DECIMAL:
                final int precision = ((DecimalType) type).getPrecision();
                final int scale = ((DecimalType) type).getScale();
                return value instanceof BigInteger
                        ? DecimalData.fromBigDecimal(
                                new BigDecimal((BigInteger) value, 0), precision, scale)
                        : DecimalData.fromBigDecimal((BigDecimal) value, precision, scale);
            case DATE:
                return (int) (((Date) value).toLocalDate().toEpochDay());
            case TIME_WITHOUT_TIME_ZONE:
                return (int) (((Time) value).toLocalTime().toNanoOfDay() / 1_000_000L);
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return TimestampData.fromTimestamp((Timestamp) value);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return TimestampData.fromInstant(((Timestamp) value).toInstant());
            case CHAR:
            case VARCHAR:
                return StringData.fromString((String) value);
            case ARRAY:
                LogicalType elementType =
                        type.getChildren().stream()
                                .findFirst()
                                .orElseThrow(
                                        () -> new RuntimeException("Unknown array element type"));
                Object[] externalArray = (Object[]) ((Array) value).getArray();
                Object[] internalArray = new Object[externalArray.length];
                for (int i = 0; i < externalArray.length; i++) {
                    internalArray[i] =
                            ClickHouseConverterUtils.toInternal(externalArray[i], elementType);
                }
                return new GenericArrayData(internalArray);
            case MAP:
                return new GenericMapData((Map<?, ?>) value);
            case ROW:
            case MULTISET:
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    public static Timestamp toFixedDateTimestamp(LocalTime localTime) {
        LocalDateTime localDateTime = localTime.atDate(DATE_PREFIX_OF_TIME);
        return Timestamp.valueOf(localDateTime);
    }
}
