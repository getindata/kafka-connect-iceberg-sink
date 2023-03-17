package com.getindata.kafka.connect.iceberg.sink.testresources;

import com.getindata.kafka.connect.iceberg.sink.IcebergSinkConfiguration;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class TestConfig {
    public static final String S3_ACCESS_KEY = "admin";
    public static final String S3_SECRET_KEY = "12345678";
    public static final String S3_BUCKET = "test-bucket";
    public static final String S3_REGION_NAME = "us-east-1";
    public static final String TABLE_NAMESPACE = "debeziumevents";
    public static final String TABLE_PREFIX = "debeziumcdc_";
    public static final String DEBEZIUM_CONNECT_IMAGE = "debezium/connect:1.9";
    public static final String POSTGRES_IMAGE = "postgres";
    public static final String MINIO_IMAGE = "minio/minio:latest";
    public static final String ZOOKEEPER_IMAGE = "confluentinc/cp-zookeeper:7.2.2";
    public static final String KAFKA_IMAGE = "confluentinc/cp-kafka:7.2.2";
    public static final String SCHEMA_REGISTRY_IMAGE = "confluentinc/cp-schema-registry:7.2.2";

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final Map<String, String> properties;

        private Builder() {
            properties = new HashMap<>();
            properties.put(IcebergSinkConfiguration.UPSERT, "true");
            properties.put(IcebergSinkConfiguration.TABLE_NAMESPACE, TABLE_NAMESPACE);
            properties.put(IcebergSinkConfiguration.TABLE_PREFIX, TABLE_PREFIX);
            properties.put(IcebergSinkConfiguration.TABLE_AUTO_CREATE, "true");
            properties.put(IcebergSinkConfiguration.CATALOG_NAME, "iceberg");
        }

        public Builder withLocalCatalog(Path localWarehouseDir) {
            properties.put(IcebergSinkConfiguration.CATALOG_TYPE, "hadoop");
            properties.put("iceberg.warehouse", localWarehouseDir.toUri().toString());
            return this;
        }

        public Builder withS3(String s3Url) {
            properties.put(IcebergSinkConfiguration.CATALOG_TYPE, "hadoop");
            properties.put("iceberg.fs.defaultFS", "s3a://" + S3_BUCKET);
            properties.put("iceberg.fs.s3a.endpoint.region", S3_REGION_NAME);
            properties.put("iceberg.warehouse", "s3a://" + S3_BUCKET + "/iceberg_warehouse");
            properties.put("iceberg.fs.s3a.access.key", S3_ACCESS_KEY);
            properties.put("iceberg.fs.s3a.secret.key", S3_SECRET_KEY);
            properties.put("iceberg.fs.s3a.path.style.access", "true");
            properties.put("iceberg.fs.s3a.endpoint", s3Url);
            return this;
        }

        public Builder withUpsert(boolean upsert) {
            properties.put(IcebergSinkConfiguration.UPSERT, Boolean.toString(upsert));
            return this;
        }

        public Builder withCustomCatalogProperty(String key, String value) {
            properties.put(IcebergSinkConfiguration.ICEBERG_PREFIX + key, value);
            return this;
        }

        public Builder withCustomProperty(String key, String value) {
            properties.put(key, value);
            return this;
        }

        public IcebergSinkConfiguration build() {
            return new IcebergSinkConfiguration(properties);
        }
    }
}
