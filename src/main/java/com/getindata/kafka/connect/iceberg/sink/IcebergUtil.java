/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package com.getindata.kafka.connect.iceberg.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.Optional;

import static org.apache.iceberg.TableProperties.*;

/**
 * @author Ismail Simsek
 */
public class IcebergUtil {
  protected static final Logger LOGGER = LoggerFactory.getLogger(IcebergUtil.class);
  protected static final ObjectMapper jsonObjectMapper = new ObjectMapper();

  public static Table createIcebergTable(Catalog icebergCatalog, TableIdentifier tableIdentifier,
      Schema schema, IcebergSinkConfiguration configuration) {

    LOGGER.info("Creating table:'{}'\nschema:{}\nrowIdentifier:{}", tableIdentifier, schema,
        schema.identifierFieldNames());

    boolean partition = !configuration.isUpsert();
    final PartitionSpec ps;
    if (partition && schema.findField(configuration.getPartitionColumn()) != null) {
      ps = PartitionSpec.builderFor(schema).day(configuration.getPartitionColumn()).build();
    } else {
      ps = PartitionSpec.builderFor(schema).build();
    }

    String formatVersion = "2";
    if (configuration.getFormatVersion() != null && !"".equals(configuration.getFormatVersion())) {
      formatVersion = configuration.getFormatVersion();
    }
    return icebergCatalog.buildTable(tableIdentifier, schema)
        .withProperties(configuration.getIcebergTableConfiguration())
        .withProperty(FORMAT_VERSION, formatVersion)
        .withSortOrder(IcebergUtil.getIdentifierFieldsAsSortOrder(schema))
        .withPartitionSpec(ps)
        .create();
  }

  private static SortOrder getIdentifierFieldsAsSortOrder(Schema schema) {
    SortOrder.Builder sob = SortOrder.builderFor(schema);
    for (String fieldName : schema.identifierFieldNames()) {
      sob = sob.asc(fieldName);
    }

    return sob.build();
  }

  public static Optional<Table> loadIcebergTable(Catalog icebergCatalog, TableIdentifier tableId) {
    try {
      Table table = icebergCatalog.loadTable(tableId);
      return Optional.of(table);
    } catch (NoSuchTableException e) {
      LOGGER.info("Table not found: {}", tableId.toString());
      return Optional.empty();
    }
  }

  public static FileFormat getTableFileFormat(Table icebergTable) {
    String formatAsString = icebergTable.properties().getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT);
    return FileFormat.valueOf(formatAsString.toUpperCase(Locale.ROOT));
  }

  public static GenericAppenderFactory getTableAppender(Table icebergTable) {
    return new GenericAppenderFactory(
        icebergTable.schema(),
        icebergTable.spec(),
        Ints.toArray(icebergTable.schema().identifierFieldIds()),
        icebergTable.schema(),
        null);
  }

  public static String toSnakeCase(String inputString) {

    StringBuilder sb = new StringBuilder();
    boolean lastUpper = true;
    boolean lastSeparator = false;

    for (Character c : inputString.toCharArray()) {

      if (Character.isUpperCase(c)) {

        if (!lastUpper) {

          if (!lastSeparator) {
            sb.append("_");
          }

          lastUpper = true;
        }

        sb.append(Character.toLowerCase(c));
        lastSeparator = false;
      }

      else {

        if (c == '_') {
          lastSeparator = true;
        }

        else {
          lastSeparator = false;
        }

        sb.append(c);
        lastUpper = false;
      }
    }

    return sb.toString();
  }
}
