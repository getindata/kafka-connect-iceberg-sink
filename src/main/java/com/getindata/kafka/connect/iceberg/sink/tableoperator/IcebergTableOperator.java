package com.getindata.kafka.connect.iceberg.sink.tableoperator;

import com.fasterxml.jackson.databind.JsonNode;
import com.getindata.kafka.connect.iceberg.sink.IcebergChangeEvent;
import com.getindata.kafka.connect.iceberg.sink.IcebergSinkConfiguration;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class IcebergTableOperator {
    private static final Logger LOGGER = LoggerFactory.getLogger(IcebergTableOperator.class);

    private final IcebergSinkConfiguration configuration;
    private final IcebergTableWriterFactory writerFactory;

    public IcebergTableOperator(IcebergSinkConfiguration configuration, IcebergTableWriterFactory writerFactory) {
        this.configuration = configuration;
        this.writerFactory = writerFactory;
    }

    /**
     * Adds list of events to iceberg table.
     * <p>
     * If field addition enabled then it groups list of change events by their schema first. Then adds new fields to
     * iceberg table if there is any. And then follows with adding data to the table.
     * <p>
     * New fields are detected using CDC event schema, since events are grouped by their schemas it uses single
     * event to find-out schema for the whole list of events.
     *
     * @param icebergTable
     * @param events
     */
    public void addToTable(Table icebergTable, List<IcebergChangeEvent> events) {

        // when operation mode is not upsert deduplicate the events to avoid inserting duplicate row
        if (configuration.isUpsert() && !icebergTable.schema().identifierFieldIds().isEmpty()) {
            events = deduplicateBatch(events);
        }

        if (!configuration.isAllowFieldAddition()) {
            // if field additions not enabled add set of events to table
            addToTablePerSchema(icebergTable, events);
        } else {
            Map<IcebergChangeEvent.JsonSchema, List<IcebergChangeEvent>> eventsGroupedBySchema =
                    events.stream()
                            .collect(Collectors.groupingBy(IcebergChangeEvent::jsonSchema));
            LOGGER.debug("Batch got {} records with {} different schema!!", events.size(), eventsGroupedBySchema.keySet().size());

            for (Map.Entry<IcebergChangeEvent.JsonSchema, List<IcebergChangeEvent>> schemaEvents : eventsGroupedBySchema.entrySet()) {
                // extend table schema if new fields found
                applyFieldAddition(icebergTable, schemaEvents.getKey().icebergSchema(configuration.getPartitionColumn()));
                // add set of events to table
                addToTablePerSchema(icebergTable, schemaEvents.getValue());
            }
        }

    }

    private List<IcebergChangeEvent> deduplicateBatch(List<IcebergChangeEvent> events) {

        ConcurrentHashMap<JsonNode, IcebergChangeEvent> icebergRecordsmap = new ConcurrentHashMap<>();

        for (IcebergChangeEvent e : events) {

            // deduplicate using key(PK) @TODO improve using map.merge
            if (icebergRecordsmap.containsKey(e.key())) {

                // replace it if it's new
                if (this.compareByTsThenOp(icebergRecordsmap.get(e.key()).value(), e.value()) <= 0) {
                    icebergRecordsmap.put(e.key(), e);
                }

            } else {
                icebergRecordsmap.put(e.key(), e);
            }

        }
        return new ArrayList<>(icebergRecordsmap.values());
    }


    /**
     * This is used to deduplicate events within given batch.
     * <p>
     * Forex ample a record can be updated multiple times in the source. for example insert followed by update and
     * delete. for this case we need to only pick last change event for the row.
     * <p>
     * Its used when `upsert` feature enabled (when the consumer operating non append mode) which means it should not add
     * duplicate records to target table.
     *
     * @param lhs
     * @param rhs
     * @return
     */
    private int compareByTsThenOp(JsonNode lhs, JsonNode rhs) {

        String upsertDedupColumn = configuration.getUpsertDedupColumn();

        int result = Long.compare(lhs.get(upsertDedupColumn).asLong(0), rhs.get(upsertDedupColumn).asLong(0));

        if (result == 0) {
            // return (x < y) ? -1 : ((x == y) ? 0 : 1);
            result = getUpsertOperation(lhs).compareByPriority(getUpsertOperation(rhs));
        }

        return result;
    }

    private CdcOperation getUpsertOperation(JsonNode lhs) {
        return CdcOperation.getByCode(lhs.get(configuration.getUpsertOpColumn()).asText("c"));
    }

    /**
     * If given schema contains new fields compared to target table schema then it adds new fields to target iceberg
     * table.
     * <p>
     * Its used when allow field addition feature is enabled.
     *
     * @param icebergTable
     * @param newSchema
     */
    private void applyFieldAddition(Table icebergTable, Schema newSchema) {

        UpdateSchema us = icebergTable.updateSchema().
                unionByNameWith(newSchema).
                setIdentifierFields(newSchema.identifierFieldNames());
        Schema newSchemaCombined = us.apply();

        // @NOTE avoid committing when there is no schema change. commit creates new commit even when there is no change!
        if (!icebergTable.schema().sameSchema(newSchemaCombined)) {
            LOGGER.info("Extending schema of {}", icebergTable.name());
            us.commit();
        }
    }

    /**
     * Adds list of change events to iceberg table. All the events are having same schema.
     *
     * @param icebergTable
     * @param events
     */
    private void addToTablePerSchema(Table icebergTable, List<IcebergChangeEvent> events) {
        // Initialize a task writer to write both INSERT and equality DELETE.
        BaseTaskWriter<Record> writer = writerFactory.create(icebergTable);
        try {
            for (IcebergChangeEvent e : events) {
                writer.write(e.asIcebergRecord(icebergTable.schema(),
                        configuration.getPartitionColumn(),
                        configuration.getPartitionTimestamp()));
            }

            writer.close();
            WriteResult result = writer.complete();
            RowDelta rowDelta = icebergTable.newRowDelta();
            Arrays.stream(result.dataFiles()).forEach(rowDelta::addRows);
            Arrays.stream(result.deleteFiles()).forEach(rowDelta::addDeletes);
            rowDelta.commit();

        } catch (IOException ex) {
            throw new ConnectException(ex);
        }

        LOGGER.debug("Committed {} events to table {}", events.size(), icebergTable.location());
    }
}
