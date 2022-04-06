package com.getindata.kafka.connect.iceberg.sink;

import com.getindata.kafka.connect.iceberg.sink.converter.SinkRecordToIcebergChangeEventConverter;
import com.getindata.kafka.connect.iceberg.sink.tableoperator.IcebergTableOperator;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.getindata.kafka.connect.iceberg.sink.IcebergSinkConfiguration.TABLE_AUTO_CREATE;

public class IcebergChangeConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(IcebergChangeConsumer.class);

    private final IcebergSinkConfiguration configuration;
    private final Catalog icebergCatalog;
    private final IcebergTableOperator icebergTableOperator;
    private final SinkRecordToIcebergChangeEventConverter converter;

    public IcebergChangeConsumer(IcebergSinkConfiguration configuration,
                                 Catalog icebergCatalog,
                                 IcebergTableOperator icebergTableOperator,
                                 SinkRecordToIcebergChangeEventConverter converter) {
        this.configuration = configuration;
        this.icebergCatalog = icebergCatalog;
        this.icebergTableOperator = icebergTableOperator;
        this.converter = converter;
    }

    public void accept(Collection<SinkRecord> records) {
        Instant start = Instant.now();

        Map<String, List<IcebergChangeEvent>> result = records.stream()
                .map(converter::convert)
                .collect(Collectors.groupingBy(IcebergChangeEvent::destinationTable));

        for (Map.Entry<String, List<IcebergChangeEvent>> event : result.entrySet()) {
            TableIdentifier tableIdentifier = TableIdentifier.of(Namespace.of(configuration.getTableNamespace()), configuration.getTablePrefix() + event.getKey());
            Table icebergTable = loadIcebergTable(icebergCatalog, tableIdentifier, event.getValue().get(0));
            icebergTableOperator.addToTable(icebergTable, event.getValue());
        }

        Instant end = Instant.now();
        LOGGER.debug("Processed {} records in {} ms", records.size(), ChronoUnit.MILLIS.between(start, end));
    }

    private Table loadIcebergTable(Catalog icebergCatalog, TableIdentifier tableId, IcebergChangeEvent sampleEvent) {
        return IcebergUtil.loadIcebergTable(icebergCatalog, tableId).orElseGet(() -> {
            if (!configuration.isTableAutoCreate()) {
                throw new ConnectException(String.format("Table '%s' not found! Set '%s' to true to create tables automatically!", tableId, TABLE_AUTO_CREATE));
            }
            return IcebergUtil.createIcebergTable(icebergCatalog, tableId, sampleEvent.icebergSchema(), configuration.getTableWriteFormat(), !configuration.isUpsert());
        });
    }
}
