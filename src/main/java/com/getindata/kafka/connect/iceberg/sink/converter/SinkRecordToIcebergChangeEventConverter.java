package com.getindata.kafka.connect.iceberg.sink.converter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.getindata.kafka.connect.iceberg.sink.IcebergChangeEvent;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;

import java.io.IOException;
import java.util.Optional;

public class SinkRecordToIcebergChangeEventConverter {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final Transformation<SinkRecord> extractNewRecordStateTransformation;
    private final JsonConverter keyJsonConverter;
    private final JsonConverter valueJsonConverter;
    private final Deserializer<JsonNode> keyDeserializer;
    private final Deserializer<JsonNode> valueDeserializer;

    public SinkRecordToIcebergChangeEventConverter(Transformation<SinkRecord> extractNewRecordStateTransformation,
                                                   JsonConverter keyJsonConverter,
                                                   JsonConverter valueJsonConverter,
                                                   Deserializer<JsonNode> keyDeserializer,
                                                   Deserializer<JsonNode> valueDeserializer) {
        this.extractNewRecordStateTransformation = extractNewRecordStateTransformation;
        this.keyJsonConverter = keyJsonConverter;
        this.valueJsonConverter = valueJsonConverter;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
    }

    public IcebergChangeEvent convert(SinkRecord record) {
        SinkRecord unwrappedRecord = extractNewRecordStateTransformation.apply(record);
        byte[] keyBytes = keyJsonConverter.fromConnectData(unwrappedRecord.topic(), unwrappedRecord.keySchema(), unwrappedRecord.key());
        byte[] valueBytes = valueJsonConverter.fromConnectData(unwrappedRecord.topic(), unwrappedRecord.valueSchema(), unwrappedRecord.value());
        JsonNode key = getValue(unwrappedRecord.topic(), keyDeserializer, keyBytes);
        JsonNode keySchema = getSchema(keyBytes);
        JsonNode value = getValue(unwrappedRecord.topic(), valueDeserializer, valueBytes);
        JsonNode valueSchema = getSchema(valueBytes);

        return new IcebergChangeEvent(unwrappedRecord.topic(), value, key, valueSchema, keySchema);
    }

    private JsonNode getValue(String topic, Deserializer<JsonNode> deserializer, byte[] bytes) {
        return Optional.ofNullable(bytes)
                .map(b -> deserializer.deserialize(topic, b))
                .orElse(null);
    }

    private JsonNode getSchema(byte[] bytes) {
        try {
            return bytes != null ? MAPPER.readTree(bytes).get("schema") : null;
        } catch (IOException e) {
            throw new ConnectException("Failed to extract schema from record", e);
        }
    }
}
