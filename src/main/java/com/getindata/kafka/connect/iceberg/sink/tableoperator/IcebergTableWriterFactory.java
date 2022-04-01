package com.getindata.kafka.connect.iceberg.sink.tableoperator;

import com.getindata.kafka.connect.iceberg.sink.IcebergSinkConfiguration;
import com.getindata.kafka.connect.iceberg.sink.IcebergUtil;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.UnpartitionedWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class IcebergTableWriterFactory {
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneOffset.UTC);
    private static final Logger LOGGER = LoggerFactory.getLogger(IcebergTableOperator.class);

    private final IcebergSinkConfiguration configuration;

    public IcebergTableWriterFactory(IcebergSinkConfiguration configuration) {
        this.configuration = configuration;
    }

    public BaseTaskWriter<Record> create(Table icebergTable) {

        FileFormat format = IcebergUtil.getTableFileFormat(icebergTable);
        GenericAppenderFactory appenderFactory = IcebergUtil.getTableAppender(icebergTable);
        int partitionId = Integer.parseInt(FORMATTER.format(Instant.now()));
        OutputFileFactory fileFactory = OutputFileFactory.builderFor(icebergTable, partitionId, 1L)
                .defaultSpec(icebergTable.spec()).format(format).build();
        List<Integer> equalityFieldIds = new ArrayList<>(icebergTable.schema().identifierFieldIds());

        BaseTaskWriter<Record> writer;
        if (icebergTable.schema().identifierFieldIds().isEmpty() || !configuration.isUpsert()) {
            if (configuration.isUpsert()) {
                LOGGER.warn("Table don't have Primary Key defined, upsert is not possible falling back to append!");
            }
            if (icebergTable.spec().isUnpartitioned()) {
                writer = new UnpartitionedWriter<>(
                        icebergTable.spec(), format, appenderFactory, fileFactory, icebergTable.io(), Long.MAX_VALUE);
            } else {
                writer = new PartitionedAppendWriter(
                        icebergTable.spec(), format, appenderFactory, fileFactory, icebergTable.io(), Long.MAX_VALUE, icebergTable.schema());
            }
        } else if (icebergTable.spec().isUnpartitioned()) {
            writer = new UnpartitionedDeltaWriter(icebergTable.spec(), format, appenderFactory, fileFactory,
                    icebergTable.io(),
                    Long.MAX_VALUE, icebergTable.schema(), equalityFieldIds, true, configuration.isUpsertKeepDelete());
        } else {
            writer = new PartitionedDeltaWriter(icebergTable.spec(), format, appenderFactory, fileFactory,
                    icebergTable.io(),
                    Long.MAX_VALUE, icebergTable.schema(), equalityFieldIds, true, configuration.isUpsertKeepDelete());
        }

        return writer;
    }
}
