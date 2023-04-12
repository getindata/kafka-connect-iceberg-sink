package com.getindata.kafka.connect.iceberg.sink;

import com.getindata.kafka.connect.iceberg.sink.converter.SinkRecordToIcebergChangeEventConverter;
import com.getindata.kafka.connect.iceberg.sink.converter.SinkRecordToIcebergChangeEventConverterFactory;
import com.getindata.kafka.connect.iceberg.sink.tableoperator.IcebergTableOperator;
import com.getindata.kafka.connect.iceberg.sink.tableoperator.IcebergTableOperatorFactory;
import org.apache.iceberg.catalog.Catalog;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

public class IcebergSinkTask extends SinkTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(IcebergSinkTask.class);
    private IcebergChangeConsumer consumer;

    @Override
    public String version() {
        return IcebergSinkVersion.getVersion();
    }

    @Override
    public void start(Map<String, String> properties) {
        LOGGER.info("Task starting");
        IcebergSinkConfiguration configuration = new IcebergSinkConfiguration(properties);
        Catalog icebergCatalog = IcebergCatalogFactory.create(configuration);
        IcebergTableOperator icebergTableOperator = IcebergTableOperatorFactory.create(configuration);
        SinkRecordToIcebergChangeEventConverter converter = SinkRecordToIcebergChangeEventConverterFactory.create(configuration);
        consumer = new IcebergChangeConsumer(configuration, icebergCatalog, icebergTableOperator, converter);
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        LOGGER.debug("Received {} records", records.size());
        consumer.accept(records);
    }

    @Override
    public void stop() {
        LOGGER.info("Task stopped");
    }
}
