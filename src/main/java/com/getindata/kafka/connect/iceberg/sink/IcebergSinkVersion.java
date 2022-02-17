package com.getindata.kafka.connect.iceberg.sink;

import java.util.Optional;

public class IcebergSinkVersion {
    public static String getVersion() {
        return Optional.ofNullable(IcebergSinkVersion.class.getPackage().getImplementationVersion())
                .orElse("unknown");
    }
}
