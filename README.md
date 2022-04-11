# Kafka Connect Iceberg Sink

Based on https://github.com/memiiso/debezium-server-iceberg

## Build

```shell
mvn clean package
```

## Usage

1. Copy `kafka-connect-iceberg-sink-0.1-SNAPSHOT-shaded.jar` into Kafka Connect plugins directory. [Kafka Connect installing plugins](https://docs.confluent.io/home/connect/self-managed/userguide.html#connect-installing-plugins)

2. POST `<kafka_connect_host>:<kafka_connect_port>/connectors`
```json
{
  "name": "iceberg-sink",
  "config": {
    "connector.class": "com.getindata.kafka.connect.iceberg.sink.IcebergSink",
    "topics": "topic1,topic2",
    
    "upsert": true,
    "upsert.keep-deletes": true,
    
    "table.auto-create": true,
    "table.write-format": "parquet",
    "table.namespace": "my_namespace",
    "table.prefix": "debeziumcdc_",
    
    "iceberg.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
    "iceberg.warehouse": "s3a://my_bucket/iceberg",
    "iceberg.fs.defaultFS": "s3a://my_bucket/iceberg",
    "iceberg.com.amazonaws.services.s3.enableV4": true,
    "iceberg.com.amazonaws.services.s3a.enableV4": true,
    "iceberg.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
    "iceberg.fs.s3a.path.style.access": true,
    "iceberg.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "iceberg.fs.s3a.access.key": "my-aws-access-key",
    "iceberg.fs.s3a.secret.key": "my-secret-access-key"
  }
}
```

### Running with debezium/connect docker image

```shell
docker run -it --name connect --net=host -p 8083:8083 \
  -e GROUP_ID=1 \
  -e CONFIG_STORAGE_TOPIC=my-connect-configs \
  -e OFFSET_STORAGE_TOPIC=my-connect-offsets \
  -e BOOTSTRAP_SERVERS=localhost:9092 \
  -e CONNECT_TOPIC_CREATION_ENABLE=true \
  -v ~/.aws/config:/kafka/.aws/config \
  -v ./target/kafka-connect-iceberg-sink-0.1-SNAPSHOT-shaded.jar:/kafka/connect/kafka-connect-iceberg-sink-0.1-SNAPSHOT-shaded.jar \
  debezium/connect
```

### Configuration reference

| Key                  | Type    | Default value  | Description                                                                                                                        |
|----------------------|---------|----------------|------------------------------------------------------------------------------------------------------------------------------------|
| upsert               | boolean | true           | When *true* Iceberg rows will be updated based on table primary key. When *false* all modification will be added as separate rows. |
| upsert.keep-deletes  | boolean | true           | When *true* delete operation will leave a tombstone that will have only a primary key and *__deleted** flag set to true            |
| upsert.dedup-column  | String  | __source_ts_ms | Column used to check which state is newer during upsert                                                                            | 
| upsert.op-column     | String  | __op           | Column used to check which state is newer during upsert when *upsert.dedup-column* is not enough to resolve                        |
| allow-field-addition | boolean | true           | When *true* sink will be adding new columns to Iceberg tables on schema changes                                                    |
| table.auto-create    | boolean | false          | When *true* sink will automatically create new Iceberg tables                                                                      |
| table.namespace      | String  | default        | Table namespace. In Glue it will be used as database name                                                                          |
| table.prefix         | String  | *empty string* | Prefix added to all table names                                                                                                    |
| table.write-format   | String  | parquet        | Format used for Iceberg tables                                                                                                     |
| iceberg.name         | String  | default        | Iceberg catalog name                                                                                                               |
| iceberg.catalog-impl | String  | *null*         | Iceberg catalog implementation (Only one of iceberg.catalog-impl and iceberg.type can be set to non null value at the same time    |
| iceberg.type         | String  | *null*         | Iceberg catalog type (Only one of iceberg.catalog-impl and iceberg.type can be set to non null value at the same time)             |
| iceberg.*            |         |                | All properties with this prefix will be passed to Iceberg Catalog implementation                                                   |

### AWS authentication

AWS credentials can be passed:
1. As part of sink configuration under keys `iceberg.fs.s3a.access.key` and `iceberg.fs.s3a.secret.key`
2. Using enviornment variables `AWS_ACCESS_KEY` and `AWS_SECRET_ACCESS_KEY`
3. As ~/.aws/config file

## Limitations

### DDL support

Full DDL support is not yet implemented, see below on supported commands and limitations.

| Command                                       | Support | Comment                                                                                                                                                                          |
|-----------------------------------------------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| CREATE TABLE                                  | +       | Reflected only when data is added.                                                                                                                                               |
| DROP TABLE                                    | !       | Does not remove structure nor data. Dropping and recreating a table with the same name is treated like modification. See below on related issues with ALTER.                     |
| ALTER TABLE … ADD                             | +       | Reflected only when data is added.                                                                                                                                               |
| ALTER TABLE .. DROP                           | !       | Does not remove structure nor data. Dropping and recreating a column with the same name is treated like modification. See below on related issues with ALTER.                    |
| ALTER TABLE … ALTER COLUMN … TYPE             | !       | Reflected only when data is added. Supported only when type is compatible e.g. int → bigint, varchar(30) → varchar(50).                                                          | 
| CREATE TABLE … (… NOT NULL …)                 | -       | All columns are marked as optional unless they are marked as primary key during table creation.                                                                                  |
| ALTER TABLE … ALTER COLUMN … SET NOT NULL     | -       | see above                                                                                                                                                                        |
| ALTER TABLE … ADD CONSTRAINT … PRIMARY KEY(…) | -       | Column that is optional cannot be changed to primary key. Sink will crash on any row update claiming that field cannot be used as identifier because it is not a required field. |

### Support for partitioned tables

Partitions are treated and reflected as completely separated tables. Example:

Given such table in PostgreSQL

```sql
CREATE TABLE dbz_part_test_1 (
    id int primary key,
    value int
) PARTITION BY RANGE (id);

CREATE TABLE dbz_part_test_1_1_11 PARTITION OF dbz_part_test_1 
    FOR VALUES FROM (1) TO (11);

CREATE TABLE dbz_part_test_1_11_20 PARTITION OF dbz_part_test_1 
    FOR VALUES FROM (11) TO (21);

INSERT INTO dbz_part_test_1 VALUES(1, 1);
INSERT INTO dbz_part_test_1 VALUES(12, 1);
```

Two tables will be created in Iceberg
- prefix_postgres_public_dbz_part_test_1_1_11
- prefix_postgres_public_dbz_part_test_1_11_20

### DML

Row cannot be updated unless table has a primary key or defined replica identity.

### Iceberg partitioning support

Currently, partitioning is done automatically based on event time. Partitioning only works when Debezium is configured in append-only mode (`upsert: false`).

Any event produced by debezium source contains a source time at which the transaction was committed:

```sql
"sourceOffset": {
  ...
  "ts_ms": "1482918357011"
}
```

From this value day part is extracted and used as partition.