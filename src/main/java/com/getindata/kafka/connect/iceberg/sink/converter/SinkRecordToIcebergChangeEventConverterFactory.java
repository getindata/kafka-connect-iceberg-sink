package com.getindata.kafka.connect.iceberg.sink.converter;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;

public class SinkRecordToIcebergChangeEventConverterFactory {
    public static SinkRecordToIcebergChangeEventConverter create() {
        Transformation<SinkRecord> extractNewRecordStateTransformation = ExtractNewRecordStateTransformationFactory.create();
        JsonConverter keyJsonConverter = JsonConverterFactory.create(true);
        JsonConverter valueJsonConverter = JsonConverterFactory.create(false);
        Deserializer<JsonNode> keyDeserializer = DeserializerFactory.create(true);
        Deserializer<JsonNode> valueDeserializer = DeserializerFactory.create(true);
        return new SinkRecordToIcebergChangeEventConverter(
                extractNewRecordStateTransformation,
                keyJsonConverter,
                valueJsonConverter,
                keyDeserializer, valueDeserializer
        );
    }
}
