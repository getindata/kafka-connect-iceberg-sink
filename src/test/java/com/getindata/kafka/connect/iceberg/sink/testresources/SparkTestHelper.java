package com.getindata.kafka.connect.iceberg.sink.testresources;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static com.getindata.kafka.connect.iceberg.sink.testresources.TestConfig.S3_ACCESS_KEY;
import static com.getindata.kafka.connect.iceberg.sink.testresources.TestConfig.S3_BUCKET;
import static com.getindata.kafka.connect.iceberg.sink.testresources.TestConfig.S3_REGION_NAME;
import static com.getindata.kafka.connect.iceberg.sink.testresources.TestConfig.S3_SECRET_KEY;
import static com.getindata.kafka.connect.iceberg.sink.testresources.TestConfig.TABLE_NAMESPACE;
import static com.getindata.kafka.connect.iceberg.sink.testresources.TestConfig.TABLE_PREFIX;

public class SparkTestHelper {
    private final SparkSession spark;

    public SparkTestHelper(String s3url) {
        SparkConf sparkconf = new SparkConf()
                .setAppName("CDC-S3-Batch-Spark-Sink")
                .setMaster("local[2]")
                .set("spark.ui.enabled", "false")
                .set("spark.eventLog.enabled", "false")
                .set("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
                .set("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
                .set("spark.hadoop.fs.s3a.endpoint", s3url)
                .set("spark.hadoop.fs.s3a.endpoint.region", S3_REGION_NAME)
                .set("spark.hadoop.fs.s3a.path.style.access", "true")
                .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
                .set("spark.sql.catalog.spark_catalog.type", "hadoop")
                .set("spark.sql.catalog.spark_catalog.warehouse", "s3a://" + S3_BUCKET + "/iceberg_warehouse")
                .set("spark.sql.warehouse.dir", "s3a://" + S3_BUCKET + "/iceberg_warehouse");

        spark = SparkSession
                .builder()
                .config(sparkconf)
                .getOrCreate();
    }

    public Dataset<Row> query(String stmt) {
        return spark.newSession().sql(stmt);
    }

    public Dataset<Row> getTableData(String table) {
        table = String.format("%s.%s", TABLE_NAMESPACE, TABLE_PREFIX) + table.replace(".", "_");
        return spark.newSession().sql("SELECT *, input_file_name() as input_file FROM " + table);
    }
}
