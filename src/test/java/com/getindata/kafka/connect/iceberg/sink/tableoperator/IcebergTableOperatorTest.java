package com.getindata.kafka.connect.iceberg.sink.tableoperator;

import com.getindata.kafka.connect.iceberg.sink.IcebergCatalogFactory;
import com.getindata.kafka.connect.iceberg.sink.IcebergChangeEvent;
import com.getindata.kafka.connect.iceberg.sink.IcebergSinkConfiguration;
import com.getindata.kafka.connect.iceberg.sink.IcebergUtil;
import com.getindata.kafka.connect.iceberg.sink.testcontainers.S3MinioContainer;
import com.getindata.kafka.connect.iceberg.sink.testresources.IcebergChangeEventBuilder;
import com.getindata.kafka.connect.iceberg.sink.testresources.MinioTestHelper;
import com.getindata.kafka.connect.iceberg.sink.testresources.SparkTestHelper;
import com.getindata.kafka.connect.iceberg.sink.testresources.TestConfig;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static com.getindata.kafka.connect.iceberg.sink.testresources.TestConfig.TABLE_NAMESPACE;
import static com.getindata.kafka.connect.iceberg.sink.testresources.TestConfig.TABLE_PREFIX;
import static com.getindata.kafka.connect.iceberg.sink.testresources.TestConfig.WRITE_FORMAT;

class IcebergTableOperatorTest {
    private static final String TEST_TABLE = "inventory.test_table_operator";

    private static IcebergTableOperator icebergTableOperator;
    private static S3MinioContainer s3MinioContainer;
    private static Catalog icebergCatalog;
    private static SparkTestHelper sparkTestHelper;

    @BeforeAll
    private static void setup() throws Exception {
        s3MinioContainer = new S3MinioContainer();
        s3MinioContainer.start();
        new MinioTestHelper(s3MinioContainer.getUrl()).createDefaultBucket();
        IcebergSinkConfiguration config = TestConfig.builder().withS3Url(s3MinioContainer.getUrl()).withUpsert(false).build();
        icebergCatalog = IcebergCatalogFactory.create(config);
        icebergTableOperator = IcebergTableOperatorFactory.create(config);
        sparkTestHelper = new SparkTestHelper(s3MinioContainer.getUrl());
    }

    public Table createTable(IcebergChangeEvent sampleEvent) {
        final TableIdentifier tableId = TableIdentifier.of(Namespace.of(TABLE_NAMESPACE), TABLE_PREFIX + sampleEvent.destinationTable());
        return IcebergUtil.createIcebergTable(icebergCatalog, tableId, sampleEvent.icebergSchema(), WRITE_FORMAT, false);
    }

    @Test
    public void testIcebergTableOperator() {
        // setup
        List<IcebergChangeEvent> events = new ArrayList<>();
        Table icebergTable = this.createTable(
                new IcebergChangeEventBuilder()
                        .destination(TEST_TABLE)
                        .addKeyField("id", 1)
                        .addField("data", "record1")
                        .addField("preferences", "feature1", true)
                        .build()
        );

        events.add(new IcebergChangeEventBuilder()
                .destination(TEST_TABLE)
                .addKeyField("id", 1)
                .addField("data", "record1")
                .build()
        );
        events.add(new IcebergChangeEventBuilder()
                .destination(TEST_TABLE)
                .addKeyField("id", 2)
                .addField("data", "record2")
                .build()
        );
        events.add(new IcebergChangeEventBuilder()
                .destination(TEST_TABLE)
                .addKeyField("id", 3)
                .addField("user_name", "Alice")
                .addField("data", "record3_adding_field")
                .build()
        );
        icebergTableOperator.addToTable(icebergTable, events);

        sparkTestHelper.getTableData(TEST_TABLE).show(false);
        Assertions.assertEquals(3, sparkTestHelper.getTableData(TEST_TABLE).count());
        events.clear();
        events.add(new IcebergChangeEventBuilder()
                .destination(TEST_TABLE)
                .addKeyField("id", 3)
                .addField("user_name", "Alice-Updated")
                .addField("data", "record3_updated")
                .addField("preferences", "feature2", "feature2Val2")
                .addField("__op", "u")
                .build()
        );
        icebergTableOperator.addToTable(icebergTable, events);
        sparkTestHelper.getTableData(TEST_TABLE).show(false);
        Assertions.assertEquals(4, sparkTestHelper.getTableData(TEST_TABLE).count());
        Assertions.assertEquals(1, sparkTestHelper.getTableData(TEST_TABLE).where("user_name == 'Alice-Updated'").count());
    }
}