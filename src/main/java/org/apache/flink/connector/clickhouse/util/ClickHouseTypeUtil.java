package org.apache.flink.connector.clickhouse.util;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.types.DataType;

import ru.yandex.clickhouse.response.ClickHouseColumnInfo;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.table.types.logical.DecimalType.MAX_PRECISION;

/** Type utils. */
public class ClickHouseTypeUtil {

    private static final Pattern INTERNAL_TYPE_PATTERN = Pattern.compile(".*?\\((?<type>.*)\\)");

    /** Convert clickhouse data type to flink data type. Whether to indicate nullable ? */
    public static DataType toFlinkType(ClickHouseColumnInfo clickHouseColumnInfo) {
        switch (clickHouseColumnInfo.getClickHouseDataType()) {
            case Int8:
                return DataTypes.TINYINT();
            case Int16:
            case UInt8:
                return DataTypes.SMALLINT();
            case Int32:
            case UInt16:
            case IntervalYear:
            case IntervalMonth:
            case IntervalWeek:
            case IntervalDay:
            case IntervalHour:
            case IntervalQuarter:
            case IntervalMinute:
            case IntervalSecond:
                return DataTypes.INT();
            case Int64:
            case UInt32:
                return DataTypes.BIGINT();
            case Int128:
            case Int256:
            case UInt64:
            case UInt128:
            case UInt256:
                return DataTypes.DECIMAL(MAX_PRECISION, 0);
            case Float32:
                return DataTypes.FLOAT();
            case Float64:
                return DataTypes.DOUBLE();
            case Decimal:
                return DataTypes.DECIMAL(
                        clickHouseColumnInfo.getPrecision(), clickHouseColumnInfo.getScale());
            case Decimal32:
                return DataTypes.DECIMAL(9, clickHouseColumnInfo.getScale());
            case Decimal64:
                return DataTypes.DECIMAL(18, clickHouseColumnInfo.getScale());
            case Decimal128:
            case Decimal256:
                return DataTypes.DECIMAL(
                        Math.min(MAX_PRECISION, clickHouseColumnInfo.getPrecision()),
                        Math.min(MAX_PRECISION, clickHouseColumnInfo.getScale()));
            case String:
            case Enum8:
            case Enum16:
                return DataTypes.STRING();
            case FixedString:
            case IPv4:
            case IPv6:
            case UUID:
                return DataTypes.VARCHAR(clickHouseColumnInfo.getPrecision());
            case Date:
                return DataTypes.DATE();
            case DateTime:
            case DateTime32:
            case DateTime64:
                return DataTypes.TIMESTAMP(clickHouseColumnInfo.getScale());
            case Array:
                String arrayBaseType =
                        getInternalClickHouseType(clickHouseColumnInfo.getOriginalTypeName());
                ClickHouseColumnInfo arrayBaseColumnInfo =
                        ClickHouseColumnInfo.parse(
                                arrayBaseType,
                                clickHouseColumnInfo.getColumnName() + ".array_base",
                                clickHouseColumnInfo.getTimeZone());
                return DataTypes.ARRAY(toFlinkType(arrayBaseColumnInfo));
            case Map:
                return DataTypes.MAP(
                        toFlinkType(clickHouseColumnInfo.getKeyInfo()),
                        toFlinkType(clickHouseColumnInfo.getValueInfo()));
            case Tuple:
            case Nested:
            case AggregateFunction:
            default:
                throw new UnsupportedOperationException(
                        "Unsupported type:" + clickHouseColumnInfo.getClickHouseDataType());
        }
    }

    private static String getInternalClickHouseType(String clickHouseTypeLiteral) {
        Matcher matcher = INTERNAL_TYPE_PATTERN.matcher(clickHouseTypeLiteral);
        if (matcher.find()) {
            return matcher.group("type");
        } else {
            throw new CatalogException(
                    String.format("No content found in the bucket of '%s'", clickHouseTypeLiteral));
        }
    }
}
