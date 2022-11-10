# Kafka Connect Iceberg Sink

Based on https://github.com/memiiso/debezium-server-iceberg

## Build

```shell
mvn clean package
```

## Usage

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


### REST / Manual based installation

1. Copy content of `kafka-connect-iceberg-sink-0.1.4-SNAPSHOT-plugin.zip` into Kafka Connect plugins directory. [Kafka Connect installing plugins](https://docs.confluent.io/home/connect/self-managed/userguide.html#connect-installing-plugins)

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
  -v ./target/plugin/kafka-connect-iceberg-sink:/kafka/connect/kafka-connect-iceberg-sink \
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

Creation of new tables and extending them with new columns is supported. Sink is not doing any operations that would affect multiple rows, because of that in case of table or column deletion no data is actually removed. This can be an issue when column is dropped and then recreated with a different type. This operation can crash the sink as it will try to write new data to a still exisitng column of a different data type.

Similar problem is with changing optionality of a column. If it was not defined as required when table was first created, sink will not check if such constrain can be introduced and will ignore that.

### DML

Rows cannot be updated nor removed unless primary key is defined. In case of deletion sink behavior is also dependent on upsert.keep-deletes option. When this option is set to true sink will leave a tombstone behind in a form of row containing only a primary key value and __deleted flat set to true. When option is set to false it will remove row entirely.

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

## Debezium change event format support

Kafka Connect Iceberg Sink is expecting events in a format of *Debezium change event*. It uses however only an *after* portion of that event and some metadata.
Minimal fields needed for the sink to work are:

Kafka event key:

```json
{
  "schema": {
    "type": "struct",
    "fields": [
      {
        "type": "int32",
        "optional": false,
        "field": "some_field"
      }
    ],
    "optional": false,
    "name": "some_event.Key"
  },
  "payload": {
    "id": 1
  }
}
```

Kafka event value:

```json
{
  "schema": {
    "type": "struct",
    "fields": [
      {
        "type": "struct",
        "fields": [
          {
            "type": "int64",
            "optional": false,
            "field": "field_name"
          },
          ...
        ],
        "optional": true,
        "name": "some_event.Value",
        "field": "after"
      },
      {
        "type": "struct",
        "fields": [
          {
            "type": "int64",
            "optional": false,
            "field": "ts_ms"
          },
          {
            "type": "string",
            "optional": false,
            "field": "db"
          },
          {
            "type": "string",
            "optional": false,
            "field": "table"
          }
        ],
        "optional": false,
        "name": "io.debezium.connector.postgresql.Source",
        "field": "source"
      },
      {
        "type": "string",
        "optional": false,
        "field": "op"
      }
    ],
    "optional": false,
    "name": "some_event.Envelope"
  },
  "payload": {
    "before": null,
    "after": {
      "some_field": 1,
      ...
    },
    "source": {
      "ts_ms": 1645448938851,
      "db": "some_source",
      "table": "some_table"
    },
    "op": "c"
  }
}
```