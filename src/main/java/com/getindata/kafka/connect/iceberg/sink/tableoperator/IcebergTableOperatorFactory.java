package com.getindata.kafka.connect.iceberg.sink.tableoperator;

import com.getindata.kafka.connect.iceberg.sink.IcebergSinkConfiguration;

public class IcebergTableOperatorFactory {
    public static IcebergTableOperator create(IcebergSinkConfiguration configuration) {
        return new IcebergTableOperator(configuration, new IcebergTableWriterFactory(configuration));
    }
}
