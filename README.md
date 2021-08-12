# Flink ClickHouse Connector

[Flink](https://github.com/apache/flink) SQL connector for [ClickHouse](https://github.com/yandex/ClickHouse) database, this project Powered by [ClickHouse JDBC](https://github.com/ClickHouse/clickhouse-jdbc).

The original code comes from AliYun. On this basis, I have done some bug fixes, code optimizations and more data type support. Currently the project only supports `Sink Table`, the `Source Table` will be implemented in the future.

## Connector Options

| Option                  | Required | Default  | Type     | Description                                                                       |
| :---------------------- | :------- | :------- | :------- | :-------------------------------------------------------------------------------- |
| url                     | required | none     | String   | The ClickHouse jdbc url in format `clickhouse://<host>:<port>`.                   |
| username                | optional | none     | String   | The 'username' and 'password' must both be specified if any of them is specified. |
| password                | optional | none     | String   | The ClickHouse password.                                                          |
| database-name           | optional | default  | String   | The ClickHouse database name.                                                     |
| table-name              | required | none     | String   | The ClickHouse table name.                                                        |
| sink.batch-size         | optional | 1000     | Integer  | The max flush size, over this will flush data.                                   |
| sink.flush-interval     | optional | 1s       | Duration | Over this flush interval mills, asynchronous threads will flush data.             |
| sink.max-retries        | optional | 3        | Integer  | The max retry times when writing records to the database failed.                  |
| sink.write-local        | optional | false    | Boolean  | Directly write data to local tables in case of distributed table.                 |
| sink.partition-strategy | optional | balanced | String   | Partition strategy: balanced(round-robin), hash(partition key), shuffle(random).  |
| sink.partition-key      | optional | none     | String   | Partition key used for hash strategy.                                             |
| sink.ignore-delete      | optional | true     | Boolean  | Whether to ignore delete statements.                                              |

## Data Type Mapping

| Flink Type          | ClickHouse Type |
| :------------------ | :-------------- |
| CHAR                | String          |
| VARCHAR             | String          |
| STRING              | String          |
| BOOLEAN             | UInt8           |
| BYTES               | FixedString     |
| DECIMAL             | Decimal         |
| TINYINT             | Int8            |
| SMALLINT            | Int16           |
| INTEGER             | Int32           |
| BIGINT              | Int64           |
| FLOAT               | Float32         |
| DOUBLE              | Float64         |
| DATE                | Date            |
| TIME                | DateTime        |
| TIMESTAMP           | DateTime        |
| TIMESTAMP_LTZ       | DateTime        |
| INTERVAL_YEAR_MONTH | Int32           |
| INTERVAL_DAY_TIME   | Int64           |
| ARRAY               | Array           |
| MAP                 | Map             |
| ROW                 | Not supported   |
| MULTISET            | Not supported   |
| RAW                 | Not supported   |

*Notice:* 

## Maven Dependency

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-connector-clickhouse</artifactId>
    <version>1.12.0-SNAPSHOT</version>
</dependency>
```

## How to use

```SQL

-- register a clickhouse table `t_user` in flink sql.
CREATE TABLE t_user (
    `user_id` BIGINT,
    `user_type` INTEGER,
    `language` STRING,
    `country` STRING,
    `gender` STRING,
    `score` DOUBLE,
    `list` ARRAY<STRING>,
    `map` Map<STRING, BIGINT>,
    PRIMARY KEY (`user_id`) NOT ENFORCED
) WITH (
    'connector' = 'clickhouse',
    'url' = 'clickhouse://{ip}:{port}',
    'database-name' = 'tutorial',
    'table-name' = 'users',
    'sink.batch-size' = '500',
    'sink.flush-interval' = '1000',
    'sink.max-retries' = '3'
);

-- write data into the clickhouse table from the table `T`
INSERT INTO t_user
SELECT cast(`user_id` as BIGINT), `user_type`, `lang`, `country`, `gender`, `score`, ARRAY['CODER', 'SPORTSMAN'], CAST(MAP['BABA', cast(10 as BIGINT), 'NIO', cast(8 as BIGINT)] AS MAP<STRING, BIGINT>) FROM T;

```

## Roadmap

- [x] Implement the Flink SQL Sink function.
- [x] Supports array and Map types.
- [ ] Implement the Flink SQL Source function.
