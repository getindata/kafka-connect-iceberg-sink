package com.getindata.kafka.connect.iceberg.sink;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Type.BOOLEAN;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

import com.getindata.kafka.connect.iceberg.sink.IcebergChangeEvent;

public class IcebergSinkConfiguration {
    public static final String UPSERT = "upsert";
    public static final String UPSERT_KEEP_DELETES = "upsert.keep-deletes";
    public static final String UPSERT_DEDUP_COLUMN = "upsert.dedup-column";
    public static final String UPSERT_OP_COLUMN = "upsert.op-column";
    public static final String ALLOW_FIELD_ADDITION = "allow-field-addition";
    public static final String TABLE_NAMESPACE = "table.namespace";
    public static final String TABLE_PREFIX = "table.prefix";
    public static final String TABLE_AUTO_CREATE = "table.auto-create";
    public static final String TABLE_SNAKE_CASE = "table.snake-case";
    public static final String COERCE_DEBEZIUM_DATE = "coerce.debezium-date";
    public static final String COERCE_DEBEZIUM_MICRO_TIMESTAMP = "coerce.debezium-micro-timestamp";
    public static final String ICEBERG_PREFIX = "iceberg.";
    public static final String ICEBERG_TABLE_PREFIX = "iceberg.table-default";
    public static final String CATALOG_NAME = ICEBERG_PREFIX + "name";
    public static final String CATALOG_IMPL = ICEBERG_PREFIX + "catalog-impl";
    public static final String CATALOG_TYPE = ICEBERG_PREFIX + "type";
    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(UPSERT, BOOLEAN, true, MEDIUM,
                    "When true Iceberg rows will be updated based on table primary key. " +
                            "When false all modification will be added as separate rows.")
            .define(UPSERT_KEEP_DELETES, BOOLEAN, true, MEDIUM,
                    "When true delete operation will leave a tombstone that will have only " +
                            "a primary key and __deleted* flag set to true")
            .define(UPSERT_DEDUP_COLUMN, STRING, "__source_ts_ms", LOW,
                    "Column used to check which state is newer during upsert")
            .define(UPSERT_OP_COLUMN, STRING, "__op", LOW,
                    "Column used to check which state is newer during upsert when " +
                            "upsert.dedup-column is not enough to resolve")
            .define(ALLOW_FIELD_ADDITION, BOOLEAN, true, LOW,
                    "When true sink will be adding new columns to Iceberg tables on schema changes")
            .define(TABLE_AUTO_CREATE, BOOLEAN, false, MEDIUM,
                    "When true sink will automatically create new Iceberg tables")
            .define(TABLE_NAMESPACE, STRING, "default", MEDIUM,
                    "Table namespace. In Glue it will be used as database name")
            .define(TABLE_PREFIX, STRING, "", MEDIUM,
                    "Prefix added to all table names")
            .define(TABLE_SNAKE_CASE, BOOLEAN, false, MEDIUM,
                    "Coerce table names to snake_case")
            .define(COERCE_DEBEZIUM_DATE, BOOLEAN, false, MEDIUM,
                    "Coerce int32 values with 'io.debezium.time.Date' annotation to local-date strings")
            .define(COERCE_DEBEZIUM_MICRO_TIMESTAMP, BOOLEAN, false, MEDIUM,
                    "Coerce int64 values with 'io.debezium.time.MicroTimestamp' annotation to" +
                            "iso datetime strings")
            .define(CATALOG_NAME, STRING, "default", MEDIUM,
                    "Iceberg catalog name")
            .define(CATALOG_IMPL, STRING, null, MEDIUM,
                    "Iceberg catalog implementation (Only one of iceberg.catalog-impl and iceberg.type " +
                            "can be set to non null value at the same time")
            .define(CATALOG_TYPE, STRING, null, MEDIUM,
                    "Iceberg catalog type (Only one of iceberg.catalog-impl and iceberg.type " +
                            "can be set to non null value at the same time)")
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

    public boolean isTableSnakeCase() {
        return parsedConfig.getBoolean(TABLE_SNAKE_CASE);
    }

    public boolean isCoerceDebeziumDate() {
        return parsedConfig.getBoolean(COERCE_DEBEZIUM_DATE);
    }

    public boolean isCoerceDebeziumMicroTimestamp() {
        return parsedConfig.getBoolean(COERCE_DEBEZIUM_MICRO_TIMESTAMP);
    }

    public String getCatalogName() {
        return parsedConfig.getString(CATALOG_NAME);
    }

    public Map<String, String> getIcebergCatalogConfiguration() {
        return getConfiguration(ICEBERG_PREFIX);
    }

    public Map<String, String> getIcebergTableConfiguration() {
        return getConfiguration(ICEBERG_TABLE_PREFIX);
    }

    private Map<String, String> getConfiguration(String prefix) {
        Map<String, String> config = new HashMap<>();
        properties.keySet().stream().filter(key -> key.startsWith(prefix)).forEach(key -> {
            config.put(key.substring(prefix.length()), properties.get(key));
        });
        return config;
    }

    public static ConfigDef getConfigDef() {
        return CONFIG_DEF;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void configureChangeEvent() {
        IcebergChangeEvent.setCoerceDebeziumDate(this.isCoerceDebeziumDate());
        IcebergChangeEvent.setCoerceDebeziumMicroTimestamp(this.isCoerceDebeziumMicroTimestamp());
    }
}
