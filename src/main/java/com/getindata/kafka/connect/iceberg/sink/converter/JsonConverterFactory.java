package com.getindata.kafka.connect.iceberg.sink.converter;

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.Converter;

import java.util.Map;

import static org.apache.kafka.connect.json.JsonConverterConfig.SCHEMAS_ENABLE_CONFIG;

public class JsonConverterFactory {
    public static JsonConverter create(boolean isKey) {
        JsonConverter jsonConverter = new JsonConverter();
        Map<String, Object> config = ImmutableMap.of(
                SCHEMAS_ENABLE_CONFIG, true
        );
        jsonConverter.configure(config, isKey);
        return jsonConverter;
    }
}
