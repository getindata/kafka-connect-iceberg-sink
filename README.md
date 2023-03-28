# Kafka Connect Iceberg Sink

Based on https://github.com/memiiso/debezium-server-iceberg

## Build

```shell
mvn clean package
```

## Usage

### Configuration reference

| Key                         | Type    | Default value    | Description                                                                                                                                                 |
| --------------------------- | ------- | ---------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| upsert                      | boolean | true             | When _true_ Iceberg rows will be updated based on table primary key. When _false_ all modification will be added as separate rows.                          |
| upsert.keep-deletes         | boolean | true             | When _true_ delete operation will leave a tombstone that will have only a primary key and \*\_\_deleted\*\* flag set to true                                |
| upsert.dedup-column         | String  | \_\_source_ts_ms | Column used to check which state is newer during upsert                                                                                                     |
| upsert.op-column            | String  | \_\_op           | Column used to check which state is newer during upsert when _upsert.dedup-column_ is not enough to resolve                                                 |
| allow-field-addition        | boolean | true             | When _true_ sink will be adding new columns to Iceberg tables on schema changes                                                                             |
| table.auto-create           | boolean | false            | When _true_ sink will automatically create new Iceberg tables                                                                                               |
| table.namespace             | String  | default          | Table namespace. In Glue it will be used as database name                                                                                                   |
| table.prefix                | String  | _empty string_   | Prefix added to all table names                                                                                                                             |
| iceberg.name                | String  | default          | Iceberg catalog name                                                                                                                                        |
| iceberg.catalog-impl        | String  | _null_           | Iceberg catalog implementation (Only one of iceberg.catalog-impl and iceberg.type can be set to non null value at the same time                             |
| iceberg.type                | String  | _null_           | Iceberg catalog type (Only one of iceberg.catalog-impl and iceberg.type can be set to non null value at the same time)                                      |
| iceberg.\*                  |         |                  | All properties with this prefix will be passed to Iceberg Catalog implementation                                                                            |
| iceberg.table-default.\*    |         |                  | Iceberg specific table settings can be changed with this prefix, e.g. 'iceberg.table-default.write.format.default' can be set to 'orc'                      |
| iceberg.partition.column    | String  | \_\_source_ts    | Column used for partitioning. If the column already exists, it must be of type timestamp.                                                                   |
| iceberg.partition.timestamp | String  | \_\_source_ts_ms | Column containing unix millisecond timestamps to be converted to partitioning times. If equal to partition.column, values will be replaced with timestamps. |
| iceberg.format-version      | String  | \_\_source_ts_ms | Specification for the Iceberg table formatg. Version 1: Analytic Data Tables. Version 2: Row-level Deletes. Default 2.                                      |

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

### Strimzi

KafkaConnect:

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaConnect
metadata:
  name: my-connect-cluster
  annotations:
    strimzi.io/use-connector-resources: "true"
spec:
  version: 3.3.1
  replicas: 1
  bootstrapServers: kafka-cluster-kafka-bootstrap:9093
  tls:
    trustedCertificates:
      - secretName: kafka-cluster-cluster-ca-cert
        certificate: ca.crt
  logging:
    type: inline
    loggers:
      log4j.rootLogger: "INFO"
      log4j.logger.com.getindata.kafka.connect.iceberg.sink.IcebergSinkTask: "DEBUG"
      log4j.logger.org.apache.hadoop.io.compress.CodecPool: "WARN"
  metricsConfig:
    type: jmxPrometheusExporter
    valueFrom:
      configMapKeyRef:
        name: connect-metrics
        key: metrics-config.yml
  config:
    group.id: my-connect-cluster
    offset.storage.topic: my-connect-cluster-offsets
    config.storage.topic: my-connect-cluster-configs
    status.storage.topic: my-connect-cluster-status
    # -1 means it will use the default replication factor configured in the broker
    config.storage.replication.factor: -1
    offset.storage.replication.factor: -1
    status.storage.replication.factor: -1
    config.providers: file,secret,configmap
    config.providers.file.class: org.apache.kafka.common.config.provider.FileConfigProvider
    config.providers.secret.class: io.strimzi.kafka.KubernetesSecretConfigProvider
    config.providers.configmap.class: io.strimzi.kafka.KubernetesConfigMapConfigProvider
  build:
    output:
      type: docker
      image: <yourdockerregistry>
      pushSecret: <yourpushSecret>
    plugins:
      - name: debezium-postgresql
        artifacts:
          - type: zip
            url: https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/2.0.0.Final/debezium-connector-postgres-2.0.0.Final-plugin.zip
      - name: iceberg
        artifacts:
          - type: zip
            url: https://github.com/TIKI-Institut/kafka-connect-iceberg-sink/releases/download/0.1.4-SNAPSHOT-hadoop-catalog-r3/kafka-connect-iceberg-sink-0.1.4-SNAPSHOT-plugin.zip
  resources:
    requests:
      cpu: "0.1"
      memory: 512Mi
    limits:
      cpu: "3"
      memory: 2Gi
  template:
    connectContainer:
      env:
        # important for using AWS s3 client sdk
        - name: AWS_REGION
          value: "none"
```

KafkaConnector Debezium Source

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaConnector
metadata:
  name: postgres-source-connector
  labels:
    strimzi.io/cluster: my-connect-cluster
spec:
  class: io.debezium.connector.postgresql.PostgresConnector
  tasksMax: 1
  config:
    tasks.max: 1
    topic.prefix: ""
    database.hostname: <databasehost>
    database.port: 5432
    database.user: <dbUser>
    database.password: <dbPassword>
    database.dbname: <databaseName>
    database.server.name: <databaseName>
    transforms: unwrap
    transforms.unwrap.type: io.debezium.transforms.ExtractNewRecordState
    transforms.unwrap.add.fields: op,table,source.ts_ms,db
    transforms.unwrap.add.headers: db
    transforms.unwrap.delete.handling.mode: rewrite
    transforms.unwrap.drop.tombstones: true
    offset.flush.interval.ms: 0
    max.batch.size: 4096 # default: 2048
    max.queue.size: 16384 # default: 8192
```

KafkaConnector Iceberg Sink:

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaConnector
metadata:
  name: iceberg-debezium-sink-connector
  labels:
    strimzi.io/cluster: my-connect-cluster
  annotations:
    strimzi.io/restart: "true"
spec:
  class: com.getindata.kafka.connect.iceberg.sink.IcebergSink
  tasksMax: 1
  config:
    topics: "<topic>"
    table.namespace: ""
    table.prefix: ""
    table.auto-create: true
    table.write-format: "parquet"
    iceberg.name: "mycatalog"
    # Nessie catalog
    iceberg.catalog-impl: "org.apache.iceberg.nessie.NessieCatalog"
    iceberg.uri: "http://nessie:19120/api/v1"
    iceberg.ref: "main"
    iceberg.authentication.type: "NONE"
    # Warehouse
    iceberg.warehouse: "s3://warehouse"
    # Minio S3
    iceberg.io-impl: "org.apache.iceberg.aws.s3.S3FileIO"
    iceberg.s3.endpoint: "http://minio:9000"
    iceberg.s3.path-style-access: true
    iceberg.s3.access-key-id: ""
    iceberg.s3.secret-access-key: ""
    # Batch size tuning
    # See: https://stackoverflow.com/questions/51753883/increase-the-number-of-messages-read-by-a-kafka-consumer-in-a-single-poll
    # And the key prefix in Note: https://stackoverflow.com/a/66551961/2688589
    consumer.override.max.poll.records: 2000 # default: 500
```

### AWS authentication

#### Hadoop s3a

AWS credentials can be passed:

1. As part of sink configuration under keys `iceberg.fs.s3a.access.key` and `iceberg.fs.s3a.secret.key`
2. Using enviornment variables `AWS_ACCESS_KEY` and `AWS_SECRET_ACCESS_KEY`
3. As ~/.aws/config file

#### Iceberg S3FileIO

https://iceberg.apache.org/docs/latest/aws/#s3-fileio

```
iceberg.warehouse: "s3://warehouse"
iceberg.io-impl: "org.apache.iceberg.aws.s3.S3FileIO"
iceberg.s3.endpoint: "http://minio:9000"
iceberg.s3.path-style-access: true
iceberg.s3.access-key-id: ''
iceberg.s3.secret-access-key: ''
```

### Catalogs

Using `GlueCatalog`

```json
{
  "name": "iceberg-sink",
  "config": {
    "connector.class": "com.getindata.kafka.connect.iceberg.sink.IcebergSink",
    "iceberg.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
    "iceberg.warehouse": "s3a://my_bucket/iceberg",
    "iceberg.fs.s3a.access.key": "my-aws-access-key",
    "iceberg.fs.s3a.secret.key": "my-secret-access-key",
    ...
  }
}
```

Using `HadoopCatalog`

```json
{
  "name": "iceberg-sink",
  "config": {
    "connector.class": "com.getindata.kafka.connect.iceberg.sink.IcebergSink",
    "iceberg.catalog-impl": "org.apache.iceberg.hadoop.HadoopCatalog",
    "iceberg.warehouse": "s3a://my_bucket/iceberg",
    ...
  }
}
```

Using `HiveCatalog`

```json
{
  "name": "iceberg-sink",
  "config": {
    "connector.class": "com.getindata.kafka.connect.iceberg.sink.IcebergSink",
    "iceberg.catalog-impl": "org.apache.iceberg.hive.HiveCatalog",
    "iceberg.warehouse": "s3a://my_bucket/iceberg",
    "iceberg.uri": "thrift://localhost:9083",
    ...
  }
}
```

## Limitations

### DDL support

Creation of new tables and extending them with new columns is supported. Sink is not doing any operations that would affect multiple rows, because of that in case of table or column deletion no data is actually removed. This can be an issue when column is dropped and then recreated with a different type. This operation can crash the sink as it will try to write new data to a still exisitng column of a different data type.

Similar problem is with changing optionality of a column. If it was not defined as required when table was first created, sink will not check if such constrain can be introduced and will ignore that.

### DML

Rows cannot be updated nor removed unless primary key is defined. In case of deletion sink behavior is also dependent on upsert.keep-deletes option. When this option is set to true sink will leave a tombstone behind in a form of row containing only a primary key value and \_\_deleted flat set to true. When option is set to false it will remove row entirely.

### Iceberg partitioning support

The consumer reads unix millisecond timestamps from the event field configured in `iceberg.partition.timestamp`, converts them to iceberg
timestamps, and writes them to the table column configured in `iceberg.partition.column`. The timestamp column is then used to extract a
date to be used as the partitioning key. If `iceberg.partition.timestamp` is empty, `iceberg.parition.column` is assumed to already be of
type timestamp, and no conversion is performed. If they are set to the same value, the integer values will be replaced by the converted
timestamp values.

Partitioning only works when configured in append-only mode (`upsert: false`).

By default, the sink expects to receive events produced by a debezium source containing a source time at which the transaction was committed:

```sql
"sourceOffset": {
  ...
  "ts_ms": "1482918357011"
}
```

## Debezium change event format support

Kafka Connect Iceberg Sink is expecting events in a format of _Debezium change event_. It uses however only an _after_ portion of that event and some metadata.
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
