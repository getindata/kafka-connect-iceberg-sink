package com.getindata.kafka.connect.iceberg.sink.tableoperator;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public enum CdcOperation {
    CREATE("c", 1),
    REPLACE("r", 2),
    UPDATE("u", 3),
    DELETE("d", 4),
    UNKNOWN("", -1);

    private static final Map<String, CdcOperation> LOOKUP_MAP = Arrays.stream(CdcOperation.values())
            .collect(Collectors.toMap(CdcOperation::getCode, o -> o));

    private String code;
    private int priority;

    CdcOperation(String code, int priority) {
        this.code = code;
        this.priority = priority;
    }

    public String getCode() {
        return code;
    }

    public int getPriority() {
        return priority;
    }

    public static CdcOperation getByCode(String code) {
        return LOOKUP_MAP.getOrDefault(code, UNKNOWN);
    }

    public int compareByPriority(CdcOperation o) {
        return Integer.compare(this.priority, o.priority);
    }
}
