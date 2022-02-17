package com.getindata.kafka.connect.iceberg.sink.converter;

import com.google.common.collect.ImmutableMap;
import io.debezium.transforms.ExtractNewRecordState;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;

public class ExtractNewRecordStateTransformationFactory {
    public static Transformation<SinkRecord> create() {
        ExtractNewRecordState<SinkRecord> extractNewRecordStateTransformation = new ExtractNewRecordState<>();
        extractNewRecordStateTransformation.configure(ImmutableMap.of(
                "add.fields", "op,table,source.ts_ms,db",
                "delete.handling.mode", "rewrite",
                "drop.tombstones", "true"
        ));
        return extractNewRecordStateTransformation;
    }
}
