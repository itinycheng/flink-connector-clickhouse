# Flink ClickHouse Connector

[Flink](https://github.com/apache/flink) SQL connector
for [ClickHouse](https://github.com/yandex/ClickHouse) database, this project Powered
by [ClickHouse JDBC](https://github.com/ClickHouse/clickhouse-jdbc).

Currently, the project supports `Source/Sink Table` and `Flink Catalog`.  
Please create issues if you encounter bugs and any help for the project is greatly appreciated.

## Connector Options

| Option                             | Required | Default  | Type     | Description                                                                                                                                                                     |
|:-----------------------------------|:---------|:---------|:---------|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                                | required | none     | String   | The ClickHouse jdbc url in format `clickhouse://<host>:<port>`.                                                                                                                 |
| username                           | optional | none     | String   | The 'username' and 'password' must both be specified if any of them is specified.                                                                                               |
| password                           | optional | none     | String   | The ClickHouse password.                                                                                                                                                        |
| database-name                      | optional | default  | String   | The ClickHouse database name.                                                                                                                                                   |
| table-name                         | required | none     | String   | The ClickHouse table name.                                                                                                                                                      |
| use-local                          | optional | false    | Boolean  | Directly read/write local tables in case of distributed table engine.                                                                                                           |
| sink.batch-size                    | optional | 1000     | Integer  | The max flush size, over this will flush data.                                                                                                                                  |
| sink.flush-interval                | optional | 1s       | Duration | Over this flush interval mills, asynchronous threads will flush data.                                                                                                           |
| sink.max-retries                   | optional | 3        | Integer  | The max retry times when writing records to the database failed.                                                                                                                |
| ~~sink.write-local~~               | optional | false    | Boolean  | Removed from version 1.15, use `use-local` instead.                                                                                                                             |
| sink.update-strategy               | optional | update   | String   | Convert a record of type UPDATE_AFTER to update/insert statement or just discard it, available: update, insert, discard.                                                        |
| sink.partition-strategy            | optional | balanced | String   | Partition strategy: balanced(round-robin), hash(partition key), shuffle(random).                                                                                                |
| sink.partition-key                 | optional | none     | String   | Partition key used for hash strategy.                                                                                                                                           |
| sink.sharding.use-table-definition | optional | false    | Boolean  | Sharding strategy consistent with definition of distributed table, if set to true, the configuration of `sink.partition-strategy` and `sink.partition-key` will be overwritten. |
| sink.ignore-delete                 | optional | true     | Boolean  | Whether to ignore delete statements.                                                                                                                                            |
| sink.parallelism                   | optional | none     | Integer  | Defines a custom parallelism for the sink.                                                                                                                                      |
| scan.partition.column              | optional | none     | String   | The column name used for partitioning the input.                                                                                                                                |
| scan.partition.num                 | optional | none     | Integer  | The number of partitions.                                                                                                                                                       |
| scan.partition.lower-bound         | optional | none     | Long     | The smallest value of the first partition.                                                                                                                                      |
| scan.partition.upper-bound         | optional | none     | Long     | The largest value of the last partition.                                                                                                                                        |
| catalog.ignore-primary-key         | optional | true     | Boolean  | Whether to ignore primary keys when using ClickHouseCatalog to create table.                                                                                                    |

**Update/Delete Data Considerations:**

1. Distributed table don't support the update/delete statements, if you want to use the
   update/delete statements, please be sure to write records to local table or set `use-local` to
   true.
2. The data is updated and deleted by the primary key, please be aware of this when using it in the
   partition table.

**breaking**
Since version 1.16, we have taken shard weight into consideration, this may affect which shard the data is distributed to.

## Data Type Mapping

| Flink Type          | ClickHouse Type                                         |
| :------------------ |:--------------------------------------------------------|
| CHAR                | String                                                  |
| VARCHAR             | String / IP / UUID                                      |
| STRING              | String / Enum                                           |
| BOOLEAN             | UInt8                                                   |
| BYTES               | FixedString                                             |
| DECIMAL             | Decimal / Int128 / Int256 / UInt64 / UInt128 / UInt256  |
| TINYINT             | Int8                                                    |
| SMALLINT            | Int16 / UInt8                                           |
| INTEGER             | Int32 / UInt16 / Interval                               |
| BIGINT              | Int64 / UInt32                                          |
| FLOAT               | Float32                                                 |
| DOUBLE              | Float64                                                 |
| DATE                | Date                                                    |
| TIME                | DateTime                                                |
| TIMESTAMP           | DateTime                                                |
| TIMESTAMP_LTZ       | DateTime                                                |
| INTERVAL_YEAR_MONTH | Int32                                                   |
| INTERVAL_DAY_TIME   | Int64                                                   |
| ARRAY               | Array                                                   |
| MAP                 | Map                                                     |
| ROW                 | Not supported                                           |
| MULTISET            | Not supported                                           |
| RAW                 | Not supported                                           |

## Maven Dependency

The project isn't published to the maven central repository, we need to deploy/install to our own
repository before use it, step as follows:

```bash
# clone the project
git clone https://github.com/itinycheng/flink-connector-clickhouse.git

# enter the project directory
cd flink-connector-clickhouse/

# display remote branches
git branch -r

# checkout the branch you need
git checkout $branch_name

# install or deploy the project to our own repository
mvn clean install -DskipTests
mvn clean deploy -DskipTests
```

```xml

<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-connector-clickhouse</artifactId>
    <version>1.16.0-SNAPSHOT</version>
</dependency>
```

## How to use

### Create and read/write table

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

-- read data from clickhouse 
SELECT user_id, user_type from t_user;

-- write data into the clickhouse table from the table `T`
INSERT INTO t_user
SELECT cast(`user_id` as BIGINT), `user_type`, `lang`, `country`, `gender`, `score`, ARRAY['CODER', 'SPORTSMAN'], CAST(MAP['BABA', cast(10 as BIGINT), 'NIO', cast(8 as BIGINT)] AS MAP<STRING, BIGINT>) FROM T;

```

### Create and use ClickHouseCatalog

#### Scala

```scala
val tEnv = TableEnvironment.create(setting)

val props = new util.HashMap[String, String]()
props.put(ClickHouseConfig.DATABASE_NAME, "default")
props.put(ClickHouseConfig.URL, "clickhouse://127.0.0.1:8123")
props.put(ClickHouseConfig.USERNAME, "username")
props.put(ClickHouseConfig.PASSWORD, "password")
props.put(ClickHouseConfig.SINK_FLUSH_INTERVAL, "30s")
val cHcatalog = new ClickHouseCatalog("clickhouse", props)
tEnv.registerCatalog("clickhouse", cHcatalog)
tEnv.useCatalog("clickhouse")

tEnv.executeSql("insert into `clickhouse`.`default`.`t_table` select...");
```

#### Java

```java
TableEnvironment tEnv = TableEnvironment.create(setting);

Map<String, String> props = new HashMap<>();
props.put(ClickHouseConfig.DATABASE_NAME, "default")
props.put(ClickHouseConfig.URL, "clickhouse://127.0.0.1:8123")
props.put(ClickHouseConfig.USERNAME, "username")
props.put(ClickHouseConfig.PASSWORD, "password")
props.put(ClickHouseConfig.SINK_FLUSH_INTERVAL, "30s");
Catalog cHcatalog = new ClickHouseCatalog("clickhouse", props);
tEnv.registerCatalog("clickhouse", cHcatalog);
tEnv.useCatalog("clickhouse");

tEnv.executeSql("insert into `clickhouse`.`default`.`t_table` select...");
```

#### SQL

```sql
> CREATE CATALOG clickhouse WITH (
    'type' = 'clickhouse',
    'url' = 'clickhouse://127.0.0.1:8123',
    'username' = 'username',
    'password' = 'password',
    'database-name' = 'default',
    'use-local' = 'false',
    ...
);

> USE CATALOG clickhouse;
> SELECT user_id, user_type FROM `default`.`t_user` limit 10;
> INSERT INTO `default`.`t_user` SELECT ...;
```

## Roadmap

- [x] Implement the Flink SQL Sink function.
- [x] Support array and Map types.
- [x] Support ClickHouseCatalog.
- [x] Implement the Flink SQL Source function.
