/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package com.getindata.kafka.connect.iceberg.sink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.getindata.kafka.connect.iceberg.sink.testresources.TestConfig;
import io.debezium.serde.DebeziumSerdes;
import io.debezium.util.Testing;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Types;
import org.apache.kafka.common.serialization.Serde;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class TestIcebergUtil {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String defaultPartitionTimestamp = "__source_ts_ms";
    private static final String defaultPartitionColumn = "__source_ts";

    final String serdeWithSchema = Testing.Files.readResourceAsString("json/serde-with-schema.json");
    final String unwrapWithSchema = Testing.Files.readResourceAsString("json/unwrap-with-schema.json");
    final String unwrapWithGeomSchema = Testing.Files.readResourceAsString("json/serde-with-schema_geom.json");
    final String unwrapWithArraySchema = Testing.Files.readResourceAsString("json/serde-with-array.json");
    final String unwrapWithArraySchema2 = Testing.Files.readResourceAsString("json/serde-with-array2.json");

    final String customPartitionColumn = Testing.Files.readResourceAsString("json/custom-partition-column.json");

    @Test
    public void testNestedJsonRecord() throws JsonProcessingException {
        IcebergChangeEvent e = new IcebergChangeEvent("test",
                MAPPER.readTree(serdeWithSchema).get("payload"), null,
                MAPPER.readTree(serdeWithSchema).get("schema"), null);
        Schema schema = e.icebergSchema(defaultPartitionColumn);
        assertTrue(schema.toString().contains("before: optional struct<2: id: optional int, 3: first_name: optional string, " +
                "4:"));
    }

    @Test
    public void testUnwrapJsonRecord() throws IOException {
        IcebergChangeEvent e = new IcebergChangeEvent("test",
                MAPPER.readTree(unwrapWithSchema).get("payload"), null,
                MAPPER.readTree(unwrapWithSchema).get("schema"), null);
        Schema schema = e.icebergSchema(defaultPartitionColumn);
        GenericRecord record = e.asIcebergRecord(schema, defaultPartitionColumn, defaultPartitionTimestamp);
        assertEquals("orders", record.getField("__table").toString());
        assertEquals(16850, record.getField("order_date"));
        System.out.println(schema);
        System.out.println(record);
    }

    @Test
    public void testNestedArrayJsonRecord() throws JsonProcessingException {
        IcebergChangeEvent e = new IcebergChangeEvent("test",
                MAPPER.readTree(unwrapWithArraySchema).get("payload"), null,
                MAPPER.readTree(unwrapWithArraySchema).get("schema"), null);
        Schema schema = e.icebergSchema(defaultPartitionColumn);
        assertTrue(schema.asStruct().toString().contains("struct<1: name: optional string, 2: pay_by_quarter: optional list<int>, 4: schedule: optional list<string>, 6:"));
        System.out.println(schema.asStruct());
        System.out.println(schema.findField("pay_by_quarter").type().asListType().elementType());
        System.out.println(schema.findField("schedule").type().asListType().elementType());
        assertEquals(schema.findField("pay_by_quarter").type().asListType().elementType().toString(), "int");
        assertEquals(schema.findField("schedule").type().asListType().elementType().toString(), "string");
        GenericRecord record = e.asIcebergRecord(schema, defaultPartitionColumn, defaultPartitionTimestamp);
        assertTrue(record.toString().contains("[10000, 10001, 10002, 10003]"));
    }

    @Test
    public void testNestedArray2JsonRecord() throws JsonProcessingException {
        assertThrows(RuntimeException.class, () -> {
            IcebergChangeEvent e = new IcebergChangeEvent("test",
                    MAPPER.readTree(unwrapWithArraySchema2).get("payload"), null,
                    MAPPER.readTree(unwrapWithArraySchema2).get("schema"), null);
            Schema schema = e.icebergSchema(defaultPartitionColumn);
            System.out.println(schema.asStruct());
            System.out.println(schema);
            System.out.println(schema.findField("tableChanges"));
            System.out.println(schema.findField("tableChanges").type().asListType().elementType());
        });
    }

    @Test
    public void testNestedGeomJsonRecord() throws JsonProcessingException {
        IcebergChangeEvent e = new IcebergChangeEvent("test",
                MAPPER.readTree(unwrapWithGeomSchema).get("payload"), null,
                MAPPER.readTree(unwrapWithGeomSchema).get("schema"), null);
        Schema schema = e.icebergSchema(defaultPartitionColumn);
        GenericRecord record = e.asIcebergRecord(schema, defaultPartitionColumn, defaultPartitionTimestamp);
        assertTrue(schema.toString().contains("g: optional struct<3: wkb: optional string, 4: srid: optional int>"));
        GenericRecord g = (GenericRecord) record.getField("g");
        GenericRecord h = (GenericRecord) record.getField("h");
        assertEquals("AQEAAAAAAAAAAADwPwAAAAAAAPA/", g.get(0, Types.StringType.get().typeId().javaClass()));
        assertEquals(123, g.get(1, Types.IntegerType.get().typeId().javaClass()));
        assertEquals("Record(null, null)", h.toString());
        assertNull(h.get(0, Types.BinaryType.get().typeId().javaClass()));
    }

    @Test
    public void testConvertPartitionTimestampRecord() throws IOException {
        IcebergChangeEvent e = new IcebergChangeEvent("test",
                MAPPER.readTree(customPartitionColumn).get("payload"), null,
                MAPPER.readTree(customPartitionColumn).get("schema"), null);
        Schema schema = e.icebergSchema(defaultPartitionColumn);
        GenericRecord record = e.asIcebergRecord(schema, defaultPartitionColumn, "timestamp");
        assertEquals("2023-03-20T18:25:27.865Z", record.getField(defaultPartitionColumn).toString());
        assertEquals("hello", record.getField("message"));
        System.out.println(schema);
        System.out.println(record);
    }

    @Test
    public void testConvertPartitionColumnRecord() throws IOException {
        IcebergChangeEvent e = new IcebergChangeEvent("test",
                MAPPER.readTree(customPartitionColumn).get("payload"), null,
                MAPPER.readTree(customPartitionColumn).get("schema"), null);
        Schema schema = e.icebergSchema("timestamp");
        GenericRecord record = e.asIcebergRecord(schema, "timestamp", "timestamp");
        assertEquals("2023-03-20T18:25:27.865Z", record.getField("timestamp").toString());
        assertEquals("hello", record.getField("message"));
        System.out.println(schema);
        System.out.println(record);
    }

    @Test
    public void valuePayloadWithSchemaAsJsonNode() {
        // testing Debezium deserializer
        final Serde<JsonNode> valueSerde = DebeziumSerdes.payloadJson(JsonNode.class);
        valueSerde.configure(Collections.emptyMap(), false);
        JsonNode deserializedData = valueSerde.deserializer().deserialize("xx", serdeWithSchema.getBytes());
        System.out.println(deserializedData.getClass().getSimpleName());
        System.out.println(deserializedData.has("payload"));
        assertEquals(deserializedData.getClass().getSimpleName(), "ObjectNode");
        System.out.println(deserializedData);
        assertTrue(deserializedData.has("after"));
        assertTrue(deserializedData.has("op"));
        assertTrue(deserializedData.has("before"));
        assertFalse(deserializedData.has("schema"));

        valueSerde.configure(Collections.singletonMap("from.field", "schema"), false);
        JsonNode deserializedSchema = valueSerde.deserializer().deserialize("xx", serdeWithSchema.getBytes());
        System.out.println(deserializedSchema);
        assertFalse(deserializedSchema.has("schema"));
    }

    @Test
    public void createIcebergTablesWithCustomProperties(@TempDir Path localWarehouseDir) {
        IcebergSinkConfiguration config = TestConfig.builder()
                .withLocalCatalog(localWarehouseDir)
                .withUpsert(false)
                .withCustomCatalogProperty("table-default.write.format.default", "orc")
                .build();

        Catalog catalog = IcebergCatalogFactory.create(config);

        Schema schema = new Schema(
                List.of(
                        Types.NestedField.required(1, "id", Types.IntegerType.get()),
                        Types.NestedField.required(2, "data", Types.StringType.get())),
                Set.of(1)
        );

        Table table1 = IcebergUtil.createIcebergTable(catalog, TableIdentifier.of("test", "test"), schema, config);

        assertTrue(IcebergUtil.getTableFileFormat(table1) == FileFormat.ORC);
    }

    @Test
    public void testToSnakeCase() {
        assertTrue(IcebergUtil.toSnakeCase("armadillo_pension").equals("armadillo_pension"));
        assertTrue(IcebergUtil.toSnakeCase("TurboPascal").equals("turbo_pascal"));
        assertTrue(IcebergUtil.toSnakeCase("Top_Of_The_Morning").equals("top_of_the_morning"));
        assertTrue(IcebergUtil.toSnakeCase("amberLetTheDogsOut").equals("amber_let_the_dogs_out"));
        assertTrue(IcebergUtil.toSnakeCase("WTF").equals("wtf"));
        assertTrue(IcebergUtil.toSnakeCase("").equals(""));
    }
}
