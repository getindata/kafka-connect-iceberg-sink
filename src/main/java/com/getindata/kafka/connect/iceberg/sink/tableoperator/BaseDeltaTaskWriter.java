package com.getindata.kafka.connect.iceberg.sink.tableoperator;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;

import java.io.IOException;
import java.util.List;

import static com.getindata.kafka.connect.iceberg.sink.tableoperator.CdcOperation.CREATE;
import static com.getindata.kafka.connect.iceberg.sink.tableoperator.CdcOperation.DELETE;

public abstract class BaseDeltaTaskWriter extends BaseTaskWriter<Record> {

    private final Schema schema;
    private final Schema deleteSchema;
    private final InternalRecordWrapper wrapper;
    private final boolean upsert;
    private final boolean upsertKeepDeletes;

    public BaseDeltaTaskWriter(PartitionSpec spec,
                               FileFormat format,
                               FileAppenderFactory<Record> appenderFactory,
                               OutputFileFactory fileFactory,
                               FileIO io,
                               long targetFileSize,
                               Schema schema,
                               List<Integer> equalityFieldIds,
                               boolean upsert,
                               boolean upsertKeepDeletes) {
        super(spec, format, appenderFactory, fileFactory, io, targetFileSize);
        this.schema = schema;
        this.deleteSchema = TypeUtil.select(schema, Sets.newHashSet(equalityFieldIds));
        this.wrapper = new InternalRecordWrapper(schema.asStruct());
        this.upsert = upsert;
        this.upsertKeepDeletes = upsertKeepDeletes;
    }

    abstract RowDataDeltaWriter route(Record row);

    InternalRecordWrapper wrapper() {
        return wrapper;
    }

    @Override
    public void write(Record row) throws IOException {
        RowDataDeltaWriter writer = route(row);
        if (upsert && !row.getField("__op").equals(CREATE.getCode())) {// anything which not an insert is upsert
            writer.delete(row);
            //System.out.println("->" + row);
        }
        // if its deleted row and upsertKeepDeletes = true then add deleted record to target table
        // else deleted records are deleted from target table
        if (
                upsertKeepDeletes
                        || !(row.getField("__op").equals(DELETE.getCode())))// anything which not an insert is upsert
        {
            writer.write(row);
        }
    }

    public class RowDataDeltaWriter extends BaseEqualityDeltaWriter {
        RowDataDeltaWriter(PartitionKey partition) {
            super(partition, schema, deleteSchema);
        }

        @Override
        protected StructLike asStructLike(Record data) {
            return wrapper.wrap(data);
        }
    }
}
