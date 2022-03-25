package com.getindata.kafka.connect.iceberg.sink;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Type.BOOLEAN;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

public class IcebergSinkConfiguration {
    public static final String UPSERT = "upsert";
    public static final String UPSERT_KEEP_DELETES = "upsert.keep-deletes";
    public static final String UPSERT_DEDUP_COLUMN = "upsert.dedup-column";
    public static final String UPSERT_OP_COLUMN = "upsert.op-column";
    public static final String ALLOW_FIELD_ADDITION = "allow-field-addition";
    public static final String TABLE_NAMESPACE = "table.namespace";
    public static final String TABLE_PREFIX = "table.prefix";
    public static final String TABLE_AUTO_CREATE = "table.auto-create";
    public static final String TABLE_WRITE_FORMAT = "table.write-format";
    public static final String ICEBERG_PREFIX = "iceberg.";
    public static final String CATALOG_NAME = ICEBERG_PREFIX + "name";
    public static final String CATALOG_IMPL = ICEBERG_PREFIX + "catalog-impl";
    public static final String CATALOG_TYPE = ICEBERG_PREFIX + "type";
    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(UPSERT, BOOLEAN, true, MEDIUM, "")
            .define(UPSERT_KEEP_DELETES, BOOLEAN, true, MEDIUM, "")
            .define(UPSERT_DEDUP_COLUMN, STRING, "__source_ts_ms", LOW, "")
            .define(UPSERT_OP_COLUMN, STRING, "__op", LOW, "")
            .define(ALLOW_FIELD_ADDITION, BOOLEAN, true, LOW, "")
            .define(TABLE_AUTO_CREATE, BOOLEAN, false, MEDIUM, "")
            .define(TABLE_NAMESPACE, STRING, "default", MEDIUM, "")
            .define(TABLE_PREFIX, STRING, "", MEDIUM, "")
            .define(TABLE_WRITE_FORMAT, STRING, "parquet", LOW, "")
            .define(CATALOG_NAME, STRING, "default", MEDIUM, "")
            .define(CATALOG_IMPL, STRING, null, MEDIUM, "")
            .define(CATALOG_TYPE, STRING, null, MEDIUM, "")
            ;

    private final AbstractConfig parsedConfig;
    private final Map<String, String> properties;

    public IcebergSinkConfiguration(Map<String, String> properties) {
        this.properties = properties;
        parsedConfig = new AbstractConfig(CONFIG_DEF, properties);
    }

    public boolean isUpsert() {
        return parsedConfig.getBoolean(UPSERT);
    }

    public boolean isUpsertKeepDelete() {
        return parsedConfig.getBoolean(UPSERT_KEEP_DELETES);
    }

    public String getUpsertDedupColumn() {
        return parsedConfig.getString(UPSERT_DEDUP_COLUMN);
    }

    public String getUpsertOpColumn() {
        return parsedConfig.getString(UPSERT_OP_COLUMN);
    }

    public boolean isAllowFieldAddition() {
        return parsedConfig.getBoolean(ALLOW_FIELD_ADDITION);
    }

    public boolean isTableAutoCreate() {
        return parsedConfig.getBoolean(TABLE_AUTO_CREATE);
    }

    public String getTableNamespace() {
        return parsedConfig.getString(TABLE_NAMESPACE);
    }

    public String getTablePrefix() {
        return parsedConfig.getString(TABLE_PREFIX);
    }

    public String getTableWriteFormat() {
        return  parsedConfig.getString(TABLE_WRITE_FORMAT);
    }
    
    public String getCatalogName() {
        return parsedConfig.getString(CATALOG_NAME);
    }
    
    public Map<String, String> getIcebergCatalogConfiguration() {
        Map<String, String> config = new HashMap<>();
        properties.keySet().stream().filter(key -> key.startsWith(ICEBERG_PREFIX)).forEach(key -> {
            config.put(key.substring(ICEBERG_PREFIX.length()), properties.get(key));
        });
        return config;
    }

    public static ConfigDef getConfigDef() {
        return CONFIG_DEF;
    }

    public Map<String, String> getProperties() {
        return properties;
    }
}
