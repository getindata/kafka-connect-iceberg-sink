/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package com.getindata.kafka.connect.iceberg.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Type.TypeID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.*;
import java.util.*;

/**
 * @author Ismail Simsek
 */
public class IcebergChangeEvent {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergChangeEvent.class);
  private final String destination;
  private final JsonNode value;
  private final JsonNode key;
  private final JsonSchema jsonSchema;
  private final IcebergSinkConfiguration configuration;

  public IcebergChangeEvent(String destination,
                            JsonNode value,
                            JsonNode key,
                            JsonNode valueSchema,
                            JsonNode keySchema, IcebergSinkConfiguration configuration) {
    this.destination = destination;
    this.value = value;
    this.key = key;
    this.configuration = configuration;
    this.jsonSchema = new JsonSchema(valueSchema, keySchema);
  }

  public JsonNode key() {
    return key;
  }

  public JsonNode value() {
    return value;
  }

  public JsonSchema jsonSchema() {
    return jsonSchema;
  }

  public Schema icebergSchema() {
    return jsonSchema.icebergSchema();
  }

  public String destinationTable() {
    return destination.replace(".", "_").replace("-", "_");
  }

  public GenericRecord asIcebergRecord(Schema schema, String partitionColumn) {
    final GenericRecord record = asIcebergRecord(schema.asStruct(), value);

    if (value != null && value.has(partitionColumn) && value.get(partitionColumn) != null) {
      final long partitionTimestamp = value.get(partitionColumn).longValue();
      final OffsetDateTime odt = OffsetDateTime.ofInstant(Instant.ofEpochMilli(partitionTimestamp), ZoneOffset.UTC);
      record.setField("__source_ts", odt);
    } else {
      record.setField("__source_ts", null);
    }
    return record;
  }

  private GenericRecord asIcebergRecord(Types.StructType tableFields, JsonNode data) {
    LOGGER.debug("Processing nested field:{}", tableFields);
    GenericRecord record = GenericRecord.create(tableFields);

    for (Types.NestedField field : tableFields.fields()) {
      // Set value to null if json event don't have the field
      if (data == null || !data.has(field.name()) || data.get(field.name()) == null) {
        record.setField(field.name(), null);
        continue;
      }
      // get the value of the field from json event, map it to iceberg value
      record.setField(field.name(), jsonValToIcebergVal(field, data.get(field.name())));
    }

    return record;
  }

  private Type.PrimitiveType icebergFieldType(String fieldType, String fieldTypeName) {
    switch (fieldType) {
      case "int8":
      case "int16":
      case "int32": // int 4 bytes
        if (configuration.isRichTemporalTypes() &&
            fieldTypeName.equals("io.debezium.time.Date")) {
          return Types.DateType.get();
        }
        else {
          return Types.IntegerType.get();
        }
      case "int64": // long 8 bytes
        if (configuration.isRichTemporalTypes() &&
            fieldTypeName.equals("io.debezium.time.MicroTimestamp")) {
          return Types.TimestampType.withoutZone();
        }
        else if (configuration.isRichTemporalTypes() &&
                 fieldTypeName.equals("io.debezium.time.MicroTime")) {
          return Types.TimeType.get();
        }
        else {
          return Types.LongType.get();
        }
      case "float8":
      case "float16":
      case "float32": // float is represented in 32 bits,
        return Types.FloatType.get();
      case "float64": // double is represented in 64 bits
        return Types.DoubleType.get();
      case "double": // double is represented in 64 bits
        return Types.DoubleType.get();
      case "boolean":
        return Types.BooleanType.get();
      case "string":
        if (configuration.isRichTemporalTypes() &&
            fieldTypeName.equals("io.debezium.time.ZonedTimestamp")) {
          return Types.TimestampType.withZone();
        }
        else if (configuration.isRichTemporalTypes() &&
                 fieldTypeName.equals("io.debezium.time.ZonedTime")) {
          return Types.TimeType.get();
        }
        else {
          return Types.StringType.get();
        }
      case "bytes":
        return Types.BinaryType.get();
      default:
        // default to String type
        return Types.StringType.get();
      //throw new RuntimeException("'" + fieldName + "' has "+fieldType+" type, "+fieldType+" not supported!");
    }
  }

  private Object jsonValToIcebergVal(Types.NestedField field,
                                     JsonNode node) {
    String fieldTypeName = field.doc();
    LOGGER.debug("Processing Field:{} Type:{} Doc:{}",
                 field.name(), field.type(), fieldTypeName);

    final Object val;
    switch (field.type().typeId()) {
      case INTEGER: // int 4 bytes
        val = node.isNull() ? null : node.asInt();
        break;
      case LONG: // long 8 bytes
        val = node.isNull() ? null : node.asLong();
        break;
      case FLOAT: // float is represented in 32 bits,
        val = node.isNull() ? null : node.floatValue();
        break;
      case DOUBLE: // double is represented in 64 bits
        val = node.isNull() ? null : node.asDouble();
        break;
      case BOOLEAN:
        val = node.isNull() ? null : node.asBoolean();
        break;
      case DATE:
        val = node.isNull() ? null
              : LocalDate.ofEpochDay(node.asInt());
        break;
      case TIMESTAMP:
        if (node.isTextual()) {
          val = OffsetDateTime.parse(node.asText());
        }
        else if (node.isNumber()) {
          Instant instant = Instant.ofEpochSecond(0L, node.asLong() * 1000);
          val = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
        }
        else if (node.isNull()){
          val = null;
        }
        else {
          throw new RuntimeException("Unrecognized JSON node type for Iceberg type TIMESTAMP: " + node.getNodeType());
        }
        break;
      case TIME:
        if (node.isLong()) {
          val = LocalTime.ofNanoOfDay(node.asLong() * 1000);
        }
        else if (node.isTextual()) {
          // Debezium converts ZonedTime values to UTC on capture, so no information is lost by converting them
          // to LocalTimes here. Iceberg doesn't support a ZonedTime equivalent anyway.
          val = OffsetTime.parse(node.asText()).toLocalTime();
        }
        else if (node.isNull()){
          val = null;
        }
        else {
          throw new RuntimeException("Unrecognized JSON node type for Iceberg type TIME: " + node.getNodeType());
        }
        break;
      case STRING:
        // if the node is not a value node (method isValueNode returns false), convert it to string.
        val = node.isValueNode() ? node.asText(null) : node.toString();
        break;
      case BINARY:
        try {
          val = node.isNull() ? null : ByteBuffer.wrap(node.binaryValue());
        } catch (IOException e) {
          LOGGER.error("Failed to convert binary value to iceberg value, field:" + field.name(), e);
          throw new RuntimeException("Failed Processing Event!", e);
        }
        break;
      case LIST:
        // for now we support two LIST type cases
        Types.ListType listType = (Types.ListType) field.type();
        if (listType.elementType().typeId() == TypeID.STRUCT) {
            List<GenericRecord> structList = new ArrayList<>();
            Iterator<JsonNode> it = node.iterator();
            while (it.hasNext()) {
                structList.add(asIcebergRecord(listType.elementType().asStructType(), it.next()));
            }
            val = structList;
        }
        else {
            val = MAPPER.convertValue(node, ArrayList.class);
        }
        break;
      case MAP:
        val = MAPPER.convertValue(node, Map.class);
        break;
      case STRUCT:
        // create it as struct, nested type
        // recursive call to get nested data/record
        val = asIcebergRecord(field.type().asStructType(), node);
        break;
      default:
        // default to String type
        // if the node is not a value node (method isValueNode returns false), convert it to string.
        val = node.isValueNode() ? node.asText(null) : node.toString();
        break;
    }

    return val;
  }

  public class JsonSchema {
    private final JsonNode valueSchema;
    private final JsonNode keySchema;

    JsonSchema(JsonNode valueSchema, JsonNode keySchema) {
      this.valueSchema = valueSchema;
      this.keySchema = keySchema;
    }

    public JsonNode valueSchema() {
      return valueSchema;
    }

    public JsonNode keySchema() {
      return keySchema;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      JsonSchema that = (JsonSchema) o;
      return Objects.equals(valueSchema, that.valueSchema) && Objects.equals(keySchema, that.keySchema);
    }

    @Override
    public int hashCode() {
      return Objects.hash(valueSchema, keySchema);
    }

    //getIcebergFieldsFromEventSchema
    private List<Types.NestedField> KeySchemaFields() {
      if (keySchema != null && keySchema.has("fields") && keySchema.get("fields").isArray()) {
        LOGGER.debug(keySchema.toString());
        return icebergSchema(keySchema, "", 0);
      }
      LOGGER.trace("Key schema not found!");
      return new ArrayList<>();
    }

    private List<Types.NestedField> valueSchemaFields() {
      if (valueSchema != null && valueSchema.has("fields") && valueSchema.get("fields").isArray()) {
        LOGGER.debug(valueSchema.toString());
        return icebergSchema(valueSchema, "", 0, true);
      }
      LOGGER.trace("Event schema not found!");
      return new ArrayList<>();
    }

    public Schema icebergSchema() {

      if (this.valueSchema == null) {
        throw new RuntimeException("Failed to get event schema, event schema is null");
      }

      final List<Types.NestedField> tableColumns = valueSchemaFields();

      if (tableColumns.isEmpty()) {
        throw new RuntimeException("Failed to get event schema, event schema has no fields!");
      }

      final List<Types.NestedField> keyColumns = KeySchemaFields();
      Set<Integer> identifierFieldIds = new HashSet<>();

      for (Types.NestedField ic : keyColumns) {
        boolean found = false;

        ListIterator<Types.NestedField> colsIterator = tableColumns.listIterator();
        while (colsIterator.hasNext()) {
          Types.NestedField tc = colsIterator.next();
          if (Objects.equals(tc.name(), ic.name())) {
            identifierFieldIds.add(tc.fieldId());
            // set column as required its part of identifier filed
            colsIterator.set(tc.asRequired());
            found = true;
            break;
          }
        }

        if (!found) {
          throw new ValidationException("Table Row identifier field `" + ic.name() + "` not found in table columns");
        }

      }

      return new Schema(tableColumns, identifierFieldIds);
    }

    private List<Types.NestedField> icebergSchema(JsonNode eventSchema, String schemaName, int columnId) {
      return icebergSchema(eventSchema, schemaName, columnId, false);
    }

    private List<Types.NestedField> icebergSchema(JsonNode eventSchema, String schemaName, int columnId,
                                                  boolean addSourceTsField) {
      List<Types.NestedField> schemaColumns = new ArrayList<>();
      String schemaType = eventSchema.get("type").textValue();
      LOGGER.debug("Converting Schema of: {}::{}", schemaName, schemaType);
      for (JsonNode jsonSchemaFieldNode : eventSchema.get("fields")) {
        columnId++;
        String fieldName = jsonSchemaFieldNode.get("field").textValue();
        String fieldType = jsonSchemaFieldNode.get("type").textValue();
        String fieldTypeName = "";
        JsonNode fieldTypeNameNode = jsonSchemaFieldNode.get("name");
        if (fieldTypeNameNode != null && !fieldTypeNameNode.isMissingNode()) {
            fieldTypeName = fieldTypeNameNode.textValue();
        }

        LOGGER.debug("Processing Field: [{}] {}.{}::{} ({})",
                     columnId, schemaName, fieldName, fieldType, fieldTypeName);
        switch (fieldType) {
          case "array":
            JsonNode items = jsonSchemaFieldNode.get("items");
            if (items != null && items.has("type")) {
              String listItemType = items.get("type").textValue();


              if (listItemType.equals("array") || listItemType.equals("map")) {
                throw new RuntimeException("'array' and 'map' nested array types are not supported," +
                                           " array[" + listItemType + "], field " + fieldName);
              }
              else {
                if (listItemType.equals("struct")) {
                  List<Types.NestedField> subSchema = icebergSchema(items, fieldName, columnId+2);
                  schemaColumns.add(Types.NestedField.optional(columnId,
                                                               fieldName,
                                                               Types.ListType.ofOptional(columnId+1,
                                                                                         Types.StructType.of(subSchema)),
                                                               ""));
                  columnId += subSchema.size() + 2;
                }
                else {
              // primitive coercions are not supported for list types, pass '""' for fieldTypeName
                  Type.PrimitiveType item = icebergFieldType(listItemType, "");
                  schemaColumns.add(Types.NestedField.optional(
                      columnId, fieldName, Types.ListType.ofOptional(++columnId, item), ""));
                }
              }
            } else {
              throw new RuntimeException("Unexpected Array type for field " + fieldName);
            }
            break;
          case "map":
            throw new RuntimeException("'" + fieldName + "' has Map type, Map type not supported!");
            //break;
          case "struct":
            // create it as struct, nested type
            // passing "" for NestedField `doc` attribute,
            // as `doc` annotated coercions are not supported for members of struct types
            List<Types.NestedField> subSchema = icebergSchema(jsonSchemaFieldNode, fieldName, columnId);
            schemaColumns.add(Types.NestedField.optional(columnId, fieldName,
                                                         Types.StructType.of(subSchema), ""));
            columnId += subSchema.size();
            break;
          default: //primitive types
            // passing fieldTypeName for NestedField `doc` attribute,
            // annotation based value coercions can be made utilizing the NestedField `doc` initializer/method
            schemaColumns.add(Types.NestedField.optional(columnId, fieldName,
                                                         icebergFieldType(fieldType, fieldTypeName),
                                                         fieldTypeName));
            break;
        }
      }

      if (addSourceTsField) {
        columnId++;
        schemaColumns.add(Types.NestedField.optional(columnId, "__source_ts",
                                                     Types.TimestampType.withZone(), ""));
      }
      return schemaColumns;
    }

  }

}
