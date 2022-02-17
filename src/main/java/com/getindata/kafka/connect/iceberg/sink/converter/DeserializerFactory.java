package com.getindata.kafka.connect.iceberg.sink.converter;

import com.fasterxml.jackson.databind.JsonNode;
import io.debezium.serde.DebeziumSerdes;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;

import java.util.Collections;

public class DeserializerFactory {
    public static Deserializer<JsonNode> create(boolean isKey) {
        Serde<JsonNode> serde = DebeziumSerdes.payloadJson(JsonNode.class);
        serde.configure(Collections.emptyMap(), isKey);
        return serde.deserializer();
    }
}
